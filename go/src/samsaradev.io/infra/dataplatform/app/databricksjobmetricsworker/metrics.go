package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/benbjohnson/clock"
	"github.com/cznic/mathutil"
	"github.com/hashicorp/go-multierror"
	"github.com/samsarahq/go/oops"
	"github.com/zorkian/go-datadog-api"
	"go.uber.org/fx"

	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/samtime"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/app/generate_terraform/util"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/datapipelines/datapipelineerrors"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/monitoring/datadoghttp"
	"samsaradev.io/infra/samsaraaws"
	"samsaradev.io/infra/serviceorchestration/leaderelection"
)

// Match against consecutive non-alpha-numeric characters, except for `-`
var disallowedCharsRegex = regexp.MustCompile(`[^a-zA-Z0-9\-()\[\]_\s]+`)
var spaceRegex = regexp.MustCompile(`\s+`)
var runDoesNotExistRegex = regexp.MustCompile(`Run [0-9]+ does not exist.`)

const JobRunsLookbackHours = 48
const DatapipelinesRunsLookbackHours = 24

type RunStatus struct {
	Status      string
	FailureType string
}

type Worker struct {
	region           string
	datadogClient    *datadog.Client
	clock            clock.Clock
	databricksClient databricks.API
}

type managedJob struct {
	jobName                string
	database               string
	table                  string
	jobType                string
	schedule               string
	sloTarget              int64
	lowUrgencyThreshold    int64
	businessHoursThreshold int64
	highUrgencyThreshold   int64
}

func New(
	lc fx.Lifecycle,
	datadogClient *datadog.Client,
	awsSession *session.Session,
	databricksClient databricks.API,
	leaderElectionHelper leaderelection.WaitForLeaderLifecycleHookHelper,
) (*Worker, error) {
	region := aws.StringValue(awsSession.Config.Region)

	w := &Worker{
		region:           region,
		datadogClient:    datadogClient,
		clock:            clock.New(),
		databricksClient: databricksClient,
	}

	lc.Append(leaderElectionHelper.RunAfterLeaderElectionLifecycleHook(func(ctx context.Context) {
		if err := w.fetchMetricsContinuously(ctx); oops.Cause(err) != context.Canceled {
			slog.Fatalw(ctx, "unexpected error running databricksjobmetricsworker", "error", err)
		}
	}))

	return w, nil
}

func logDuration(ctx context.Context, name string, timeMs int64) {
	prefix := ""
	if samsaraaws.IsRunningInLocalDev() {
		prefix = "dev."
	}
	slog.Infow(ctx, fmt.Sprintf("operation %s took %d seconds", name, timeMs/1000))
	monitoring.AggregatedDatadog.Gauge(float64(timeMs), fmt.Sprintf("%sdatabricksjobmetrics.%s", prefix, name))
}

func (w *Worker) fetchMetricsContinuously(ctx context.Context) error {
	if samsaraaws.IsRunningInLocalDev() {
		slog.Infow(ctx, "Running in Local Dev; Metrics will be prefixed with `dev.` to not overwrite prod metrics")
	}
	errorCount := 0
	for {
		// skiplint: +loopedexpensivecall
		if err := w.collectAndPostMetrics(ctx); err != nil {
			monitoring.AggregatedDatadog.Incr("databricksjobmetrics.run", "success:false")
			errorCount += 1
			if errorCount >= 3 {
				return oops.Wrapf(err, "failed to post metrics 3 times in a row.")
			} else {
				slog.Warnw(ctx, fmt.Sprintf("failed to collect and post metrics, but will retry again. errorcount: %d. error: %v", errorCount, err))
			}
		} else {
			monitoring.AggregatedDatadog.Incr("databricksjobmetrics.run", "success:true")
		}
		// Sleep in between runs to space out calls to the DBX API to avoid any rate limit issues or
		// infinite looping if the API is down.
		slog.Infow(ctx, "Sleeping 5 minutes between runs")
		if err := aws.SleepWithContext(ctx, time.Minute*5); err != nil {
			return oops.Wrapf(err, "sleeping between fetching metrics")
		}
	}
}

func (w *Worker) collectAndPostMetrics(ctx context.Context) error {
	start := w.clock.Now()
	defer func() {
		elapsed := samtime.TimeToMs(w.clock.Now()) - samtime.TimeToMs(start)
		logDuration(ctx, "total_duration", elapsed)
	}()

	var allMetrics []datadog.Metric
	var collectedErrors *multierror.Error

	// Fetch Job Metrics.
	jobMetricFetchStart := w.clock.Now()
	slog.Infow(ctx, "Starting to collect job metrics.")
	jobMetrics, err := w.fetchJobMetrics(ctx)
	slog.Infow(ctx, "Fetched job metrics", "count", len(jobMetrics))
	if err != nil {
		slog.Errorw(ctx, oops.Wrapf(err, "error fetching job metrics"), nil)
		collectedErrors = multierror.Append(collectedErrors, err)
	}
	allMetrics = append(allMetrics, jobMetrics...)
	logDuration(ctx, "job_metrics_fetch_duration", samtime.TimeToMs(w.clock.Now())-samtime.TimeToMs(jobMetricFetchStart))

	// Fetch Datapipelines Metrics.
	datapipelinesMetricFetchStart := w.clock.Now()
	slog.Infow(ctx, "Starting to collect datapipelines metrics.")
	datapipelinesMetrics, err := w.fetchDatapipelinesMetrics(ctx)
	slog.Infow(ctx, "Fetched datapipelines metrics", "count", len(datapipelinesMetrics))
	if err != nil {
		slog.Errorw(ctx, oops.Wrapf(err, "error fetching job metrics"), nil)
		collectedErrors = multierror.Append(collectedErrors, err)
	}
	allMetrics = append(allMetrics, datapipelinesMetrics...)
	logDuration(ctx, "datapipelines_metrics_fetch_duration", samtime.TimeToMs(w.clock.Now())-samtime.TimeToMs(datapipelinesMetricFetchStart))

	// Fetch instance pool metrics.
	instancePoolMetricFetchStart := w.clock.Now()
	slog.Infow(ctx, "Starting to collect instance pool metrics.")
	instancePoolMetrics, err := w.fetchInstancePoolMetrics(ctx)
	slog.Infow(ctx, "Fetched instance pool metrics", "count", len(instancePoolMetrics))
	if err != nil {
		slog.Errorw(ctx, oops.Wrapf(err, "error fetching instance pool metrics"), nil)
		collectedErrors = multierror.Append(collectedErrors, err)
	}
	allMetrics = append(allMetrics, instancePoolMetrics...)
	logDuration(ctx, "instance_pools_metrics_fetch_duration", samtime.TimeToMs(w.clock.Now())-samtime.TimeToMs(instancePoolMetricFetchStart))

	// Even if we encounter errors, we still want to post whatever
	// metrics we did get.
	// Limit metrics per POST to avoid hitting size limit.
	slog.Infow(ctx, "Starting to send metrics.")
	datadogStart := w.clock.Now()
	const pageSize = 1000
	for i := 0; i*pageSize < len(allMetrics); i++ {
		page := allMetrics[i*pageSize : mathutil.Min((i+1)*pageSize, len(allMetrics))]
		if err := w.datadogClient.PostMetrics(page); err != nil {
			slog.Errorw(ctx, oops.Wrapf(err, "post metrics"), nil)
			collectedErrors = multierror.Append(collectedErrors, err)
		}
	}

	slog.Infow(ctx, "Done sending metrics.")
	logDuration(ctx, "datadog_post_duration", samtime.TimeToMs(w.clock.Now())-samtime.TimeToMs(datadogStart))

	return collectedErrors.ErrorOrNil()
}

func classifyRunStatus(runState databricks.RunState) (RunStatus, error) {
	var runStatus RunStatus
	var errors error
	switch runState.LifeCycleState {
	case databricks.RunLifeCycleStateTerminated:
		if runState.ResultState != nil {
			switch *runState.ResultState {
			case databricks.RunResultStateSuccess:
				runStatus.Status = "success"
			case databricks.RunResultStateFailed, databricks.RunResultStateTimedout, databricks.RunResultStateCanceled, databricks.RunResultStateSuccessWithFailures:
				runStatus.Status = "failure"
				runStatus.FailureType = strings.ToLower(string(*runState.ResultState))
			default:
				runStatus.Status = "unknown"
				errors = multierror.Append(errors, oops.Errorf("unknown run result state %s.\n", *runState.ResultState))
			}
		} else {
			runStatus.Status = "unknown"
			errors = multierror.Append(errors, oops.Errorf("no run result state"))
		}
	case databricks.RunLifeCycleStateInternalError:
		if runState.ResultState != nil {
			runStatus.Status = "failure"
			switch *runState.ResultState {
			case databricks.RunResultStateFailed, databricks.RunResultStateTimedout:
				runStatus.FailureType = strings.ToLower(string(*runState.ResultState))
			default:
				runStatus.FailureType = "internal"
			}
		} else {
			runStatus.Status = "error"
			runStatus.FailureType = "internal"
			return runStatus, errors
		}
	case databricks.RunLifeCycleStatePending, databricks.RunLifeCycleStateBlocked, databricks.RunLifeCycleStateWaitingForRetry:
		runStatus.Status = "waiting"
	case databricks.RunLifeCycleStateRunning, databricks.RunLifeCycleStateTerminating:
		runStatus.Status = "running"
	case databricks.RunLifeCycleStateSkipped:
		runStatus.Status = "skipped"
	}
	return runStatus, errors
}

func makeTaskRunMetrics(ctx context.Context, run *databricks.Run, existingClusters map[string]databricks.ClusterSpec, pools map[string]databricks.InstancePoolAndStats, tags []string, jobTags map[string]string) ([]datadog.Metric, error) {
	var metrics []datadog.Metric
	var errors error

	jobClustersByKey := map[string]*databricks.JobCluster{}
	if len(run.JobClusters) > 0 {
		for _, jc := range run.JobClusters {
			jobClustersByKey[jc.JobClusterKey] = jc
		}
	}

	for _, taskRun := range run.Tasks {

		taskRunStatus, err := classifyRunStatus(taskRun.State)
		if err != nil {
			errors = multierror.Append(errors, err)
		}
		if taskRunStatus.Status == "unknown" {
			continue
		}
		cleanedTaskKey := cleanName(taskRun.TaskKey)
		// Though we are adding many tags, they're mostly related and so we don't expect the total number of unique
		// tags to grow out of control. Namely, the max we ever expect to see is:
		// 2 (regions) * <# jobs (~2500)> * 2 (statuses) * 4 (failure_type)
		// with most of these probably never happening, since many jobs aren't in both regions, won't fail in every
		// way, etc.
		taskMetricsTags := append(tags,
			[]string{
				fmt.Sprintf("status:%s", taskRunStatus.Status),
				fmt.Sprintf("failure_type:%s", taskRunStatus.FailureType),
				fmt.Sprintf("attempt_number:%d", taskRun.AttemptNumber),
				fmt.Sprintf("task_name:%s", cleanedTaskKey),
			}...,
		)

		sparkVersion := ""
		clusterName := ""
		clusterInfo := getTaskRunCluster(ctx, *taskRun, jobClustersByKey, existingClusters)
		if clusterInfo != nil {
			sparkVersion = string(clusterInfo.SparkVersion)
			clusterName = clusterInfo.ClusterName
		}

		if taskRun.ExistingClusterId != "" {
			sparkVersion = "unknown"
			taskMetricsTags = append(taskMetricsTags,
				[]string{
					fmt.Sprintf("existing_cluster_id:%s", taskRun.ExistingClusterId),
					fmt.Sprintf("existing_cluster_name:%s", clusterName),
				}...,
			)
		}
		taskMetricsTags = append(taskMetricsTags, fmt.Sprintf("spark_version:%s", sparkVersion))

		samsaraTags := getSamsaraTags(ctx, pools, clusterInfo, jobTags)

		taskMetricsTags = append(taskMetricsTags, samsaraTags...)

		sort.Strings(taskMetricsTags)

		metricPrefix := ""
		if samsaraaws.IsRunningInLocalDev() {
			// Make the metric prefix `dev.` from local to allow local testing and not overwrite the prod metric.
			metricPrefix = "dev."
		}

		metrics = append(metrics,
			datadoghttp.CounterMetric(samtime.MsToTime(taskRun.EndTime), metricPrefix+"databricks.jobs.task.run.finish", 1.0, taskMetricsTags, 1.0),
			datadoghttp.GaugeMetric(samtime.MsToTime(taskRun.EndTime), metricPrefix+"databricks.jobs.task.run.setup_duration", float64(taskRun.SetupDuration), taskMetricsTags),
			datadoghttp.GaugeMetric(samtime.MsToTime(taskRun.EndTime), metricPrefix+"databricks.jobs.task.run.execution_duration", float64(taskRun.ExecutionDuration), taskMetricsTags),
			datadoghttp.GaugeMetric(samtime.MsToTime(taskRun.EndTime), metricPrefix+"databricks.jobs.task.run.cleanup_duration", float64(taskRun.CleanupDuration), taskMetricsTags),
			datadoghttp.GaugeMetric(samtime.MsToTime(taskRun.EndTime), metricPrefix+"databricks.jobs.task.run.duration", float64(taskRun.EndTime-taskRun.StartTime), taskMetricsTags),
		)
	}

	return metrics, errors
}

func getTaskRunCluster(ctx context.Context, taskRun databricks.RunTask, jobClusters map[string]*databricks.JobCluster, existingClusters map[string]databricks.ClusterSpec) *databricks.ClusterSpec {
	var clusterInfo *databricks.ClusterSpec
	if jc, ok := jobClusters[taskRun.JobClusterKey]; taskRun.JobClusterKey != "" && ok {
		clusterInfo = jc.NewCluster
	} else if taskRun.NewCluster != nil {
		clusterInfo = taskRun.NewCluster
	}

	if taskRun.ExistingClusterId != "" {
		existingCluster, ok := existingClusters[taskRun.ExistingClusterId]
		if !ok {
			slog.Warnw(ctx, "failed to find existing cluster name", "cluster_id", taskRun.ExistingClusterId, "run_id", taskRun.RunId, "run_url", taskRun.RunPageUrl)
		} else {
			clusterInfo = &existingCluster
		}
	}

	return clusterInfo
}

func (w *Worker) fetchJobMetrics(ctx context.Context) ([]datadog.Metric, error) {
	var metrics []datadog.Metric
	var errors error
	baseTags := []string{
		fmt.Sprintf("region:%s", w.region),
	}

	timerStart := samtime.TimeToMs(w.clock.Now())
	jobs, err := w.databricksClient.ListJobs(ctx, &databricks.ListJobsInput{ExpandTasks: true})
	if err != nil {
		return nil, oops.Wrapf(err, "list jobs")
	}
	slog.Infow(ctx, "finished listing jobs")
	logDuration(ctx, "list_jobs_duration", samtime.TimeToMs(w.clock.Now())-timerStart)

	timerStart = samtime.TimeToMs(w.clock.Now())
	pools, err := w.databricksClient.ListInstancePools(ctx)
	if err != nil {
		return nil, oops.Wrapf(err, "list instance pools")
	}
	slog.Infow(ctx, "finished listing pools")
	logDuration(ctx, "list_pools_duration", samtime.TimeToMs(w.clock.Now())-timerStart)

	// Note that this API is a bit weird and only returns pinned clusters, active clusters, and then recently terminated clusters. See https://docs.databricks.com/dev-tools/api/latest/clusters.html#list
	// We rely on the fact that all our interactive clusters are pinned, that way we know they should be in this response.
	// If they are not, then we'll only tag the metric with the cluster ID, not the name.
	timerStart = samtime.TimeToMs(w.clock.Now())
	clusters, err := w.databricksClient.ListClusters(ctx, &databricks.ListClustersInput{})
	if err != nil {
		return nil, oops.Wrapf(err, "list clusters")
	}
	slog.Infow(ctx, "finished listing clusters\n")
	logDuration(ctx, "list_clusters_duration", samtime.TimeToMs(w.clock.Now())-timerStart)

	poolsById := make(map[string]databricks.InstancePoolAndStats)
	for _, pool := range pools.InstancePools {
		poolsById[pool.InstancePoolId] = pool
	}

	jobsById := make(map[int64]databricks.Job)
	for _, job := range jobs.Jobs {
		jobsById[job.JobId] = job
	}

	// Look through the jobs for the managed jobs we want to keep track of.
	managedJobsById := make(map[int64]managedJob)
	for _, job := range jobs.Jobs {

		// If the job has a cluster or job tags (serverless jobs), it's a managed job.
		if len(job.JobSettings.JobClusters) > 0 || (job.JobSettings.Tags != nil && len(job.JobSettings.Tags) > 0) {
			var clusterSpec *databricks.ClusterSpec
			if len(job.JobSettings.JobClusters) > 0 {
				clusterSpec = job.JobSettings.JobClusters[0].NewCluster
			}
			tags := getSamsaraTags(ctx, poolsById, clusterSpec, job.JobSettings.Tags)

			mj := managedJob{
				jobName: cleanName(job.JobSettings.Name),
			}

			for _, tag := range tags {
				parts := strings.Split(tag, ":")
				if len(parts) != 2 {
					slog.Warnw(ctx, "failed to parse tag", "tag", tag, "job", job.JobId)
					continue
				}
				name := parts[0]
				value := parts[1]

				switch name {
				case dataplatformconsts.DATABASE_TAG:
					mj.database = value
				case dataplatformconsts.TABLE_TAG:
					mj.table = value
				case dataplatformconsts.DATAPLAT_JOBTYPE_TAG:
					mj.jobType = value
				case dataplatformconsts.SCHEDULE_TAG:
					mj.schedule = value
				case dataplatformconsts.SLO_TARGET_TAG:
					mj.sloTarget = parseIntOrZero(value)
				case dataplatformconsts.LOW_URGENCY_THRESHOLD_TAG:
					mj.lowUrgencyThreshold = parseIntOrZero(value)
				case dataplatformconsts.BUSINESS_HOURS_THRESHOLD_TAG:
					mj.businessHoursThreshold = parseIntOrZero(value)
				case dataplatformconsts.HIGH_URGENCY_THRESHOLD_TAG:
					mj.highUrgencyThreshold = parseIntOrZero(value)
				}
			}

			// Track anything that has a dataplat job type enabled on it.
			if mj.jobType != "" {
				managedJobsById[job.JobId] = mj
			}
		}
	}

	// From our managed job list, map to the time of the most recent successful run.
	mostRecentSuccesses := make(map[int64]int64)
	for id := range managedJobsById {
		mostRecentSuccesses[id] = -1
	}

	clustersById := make(map[string]databricks.ClusterSpec)
	for _, cluster := range clusters.Clusters {
		clustersById[cluster.ClusterId] = cluster
	}

	// Append prefix `dev.` to metrics to allow local testing and not overwrite prod metrics.
	metricPrefix := ""
	if samsaraaws.IsRunningInLocalDev() {
		metricPrefix = "dev."
	}

	// ### WORKSPACE LEVEL METRICS ###
	// Log the total number of jobs
	metrics = append(
		metrics,
		datadoghttp.GaugeMetric(w.clock.Now(), metricPrefix+"databricks.jobs.count", float64(len(jobs.Jobs)), baseTags),
	)

	activeRuns, err := w.databricksClient.ListRuns(ctx, &databricks.ListRunsInput{
		ActiveOnly: true,
	})

	if err != nil {
		return nil, oops.Wrapf(err, "active jobs")
	}

	// Log the total number of active jobs.
	metrics = append(
		metrics,
		datadoghttp.GaugeMetric(w.clock.Now(), metricPrefix+"databricks.jobs.active.count", float64(len(activeRuns.Runs)), baseTags),
	)

	activeClusters, err := w.databricksClient.ListClustersV2(ctx, &databricks.ListClustersInputV2{
		FilterBy: databricks.FilterBy{
			ClusterStates: []string{"PENDING", "RUNNING"},
		},
	})

	if err != nil {
		return nil, oops.Wrapf(err, "active clusters")
	}

	// Log the total number of active clusters.
	metrics = append(
		metrics,
		datadoghttp.GaugeMetric(w.clock.Now(), metricPrefix+"databricks.clusters.active.count", float64(len(activeClusters.Clusters)), baseTags),
	)

	// Fetch all the runs in the last couple days, and emit metrics for them.
	// Datadog says that custom metrics can only be logged up to 1 hour in the past, so we don't
	// need to fetch that many. (though, in testing, it seemed like it accepted up to 4 hrs in the past)
	timerStart = samtime.TimeToMs(w.clock.Now())
	slog.Infow(ctx, "calling DBX API for ListRuns")

	// Fetches all the runs in the last 48 hours.
	// Unfortunately, the databricks list runs api is sorted in descending order by
	// start time, which is not helpful for our incremental use case.
	// Instead we'll just make the assumption that no job runs more than 2 days long.
	// This fetches only the databricks UI run jobs. Subset of submit_run jobs are processed in FetchDatapipelinesMetrics.
	runs, err := w.getAllRuns(ctx, JobRunsLookbackHours, databricks.RunTypeJobRun)
	if err != nil {
		return nil, oops.Wrapf(err, "failed getting JOB_RUN runs")
	}
	slog.Infow(ctx, fmt.Sprintf("fetched %d runs", len(runs)))
	logDuration(ctx, "list_runs_duration", samtime.TimeToMs(w.clock.Now())-timerStart)

	timerStart = samtime.TimeToMs(w.clock.Now())
	for _, run := range runs {

		// Grab the job related to this run so that we can fetch tags and other relevant information.
		job, ok := jobsById[run.JobId]
		if !ok {
			slog.Warnw(ctx, "failed to find job for run. skipping...", "run_id", run.RunId, "job_id", run.JobId, "run_url", run.RunPageUrl)
			continue
		}

		// Get the cluster keys for this workflow to help piece together
		// cluster information per task.
		jobClustersByKey := map[string]*databricks.JobCluster{}
		for _, jc := range run.JobClusters {
			jobClustersByKey[jc.JobClusterKey] = jc
		}

		// There are limitations on tags, so clean the job name before.
		cleanedJobName := cleanName(job.JobSettings.Name)
		isProduction := false
		jobType := ""
		metricTags := append([]string{
			fmt.Sprintf("job_id:%d", job.JobId),
			fmt.Sprintf("job_name:%s", cleanedJobName),
		}, baseTags...,
		)

		// Build metrics on a per-task basis.
		taskRunMetrics, errs := makeTaskRunMetrics(ctx, run, clustersById, poolsById, metricTags, job.JobSettings.Tags)
		if errs != nil {
			errors = multierror.Append(errors, errs)
		}
		metrics = append(metrics, taskRunMetrics...)

		// Build metrics for the overall job status.
		jobRunStatus, errs := classifyRunStatus(run.State)
		if errs != nil {
			errors = multierror.Append(errors, errs)
		}
		if jobRunStatus.Status == "unknown" {
			continue
		}

		// Note: the `setupDuration`, `cleanupDuration`, and `executionDuration` on the overall job
		// are not set. We have to sum up task-level metrics to get them.
		// TODO(parth): I think this above statement is true but just double check.
		totalSetupDuration := 0
		totalCleanupDuration := 0
		totalExecutionDuration := 0
		samsaraTags := map[string]struct{}{}
		for _, taskRun := range run.Tasks {
			totalSetupDuration += int(taskRun.SetupDuration)
			totalCleanupDuration += int(taskRun.CleanupDuration)
			totalExecutionDuration += int(taskRun.ExecutionDuration)

			clusterInfo := getTaskRunCluster(ctx, *taskRun, jobClustersByKey, clustersById)

			tags := getSamsaraTags(ctx, poolsById, clusterInfo, job.JobSettings.Tags)
			for _, t := range tags {
				samsaraTags[t] = struct{}{}
				if t == dataplatformconsts.PRODUCTION_TAG+":true" {
					isProduction = true
				} else if strings.Contains(t, dataplatformconsts.DATAPLAT_JOBTYPE_TAG) {
					parts := strings.Split(t, ":")
					jobType = parts[len(parts)-1]
				}
			}
		}

		jobMetricsTags := append(
			[]string{
				fmt.Sprintf("status:%s", jobRunStatus.Status),
				fmt.Sprintf("failure_type:%s", jobRunStatus.FailureType),
				fmt.Sprintf("job_id:%d", job.JobId),
				fmt.Sprintf("job_name:%s", cleanedJobName),
			},
			baseTags...,
		)
		for k := range samsaraTags {
			jobMetricsTags = append(jobMetricsTags, k)
		}

		// If this is a managed job, lets log the run time if it was successful.
		if _, ok := managedJobsById[run.JobId]; ok && jobRunStatus.Status == "success" {
			if run.EndTime > mostRecentSuccesses[job.JobId] {
				mostRecentSuccesses[job.JobId] = run.EndTime
			}
		}

		if isProduction && jobType != "" {
			switch jobType {
			case string(dataplatformconsts.KinesisStatsDeltaLakeIngestionMerge), string(dataplatformconsts.RdsDeltaLakeIngestionMerge), string(dataplatformconsts.KinesisStatsBigStatsDeltaLakeIngestionMerge):
				// Merge jobs start every 3 hours from 0:00 UTC time. So we should check if start hour is not the same as end hour.
				startHour := samtime.MsToTime(run.StartTime).Hour()
				endHour := samtime.MsToTime(run.EndTime).Hour()
				if startHour%3 == 0 && startHour != endHour {
					// This job has overrun! Let's log it
					metrics = append(
						metrics,
						datadoghttp.CounterMetric(samtime.MsToTime(run.EndTime), metricPrefix+"databricks.jobs.run.merge_job_overrun", 1.0, jobMetricsTags, 1.0),
					)
					jobMetricsTags = append(jobMetricsTags, "job_overrun:true")
				}
				jobMetricsTags = append(jobMetricsTags, "job_overrun:false")
			}
		}
		sort.Strings(jobMetricsTags)
		metrics = append(
			metrics,
			datadoghttp.CounterMetric(samtime.MsToTime(run.EndTime), metricPrefix+"databricks.jobs.run.finish", 1.0, jobMetricsTags, 1.0),
			datadoghttp.GaugeMetric(samtime.MsToTime(run.EndTime), metricPrefix+"databricks.jobs.run.setup_duration", float64(totalSetupDuration), jobMetricsTags),
			datadoghttp.GaugeMetric(samtime.MsToTime(run.EndTime), metricPrefix+"databricks.jobs.run.execution_duration", float64(totalExecutionDuration), jobMetricsTags),
			datadoghttp.GaugeMetric(samtime.MsToTime(run.EndTime), metricPrefix+"databricks.jobs.run.cleanup_duration", float64(totalCleanupDuration), jobMetricsTags),
			// Total duration is wall clock time, end-start
			datadoghttp.GaugeMetric(samtime.MsToTime(run.EndTime), metricPrefix+"databricks.jobs.run.duration", float64(run.EndTime-run.StartTime), jobMetricsTags),
		)
	}
	logDuration(ctx, "process_runs_duration", samtime.TimeToMs(w.clock.Now())-timerStart)

	// Emit metrics for the time since the last successful run for each managed job.
	for jobId, mj := range managedJobsById {
		tags := w.tagsForManagedJob(jobId, mj)
		mostRecent, ok := mostRecentSuccesses[jobId]
		if !ok {
			mostRecent = -1
		}

		// There are cases where there no successful runs in the last 2 days. Given how rarely we expect
		// this to happen, we can call the runs api for this specific job to find the last successful run.
		if mostRecent == -1 {
			slog.Infow(ctx, "no successful runs in last 2 days, searching for last successful run by directly asking the api", "job_id", jobId, "database", mj.database, "table", mj.table, "schedule", mj.schedule)
			history, err := w.databricksClient.ListRuns(ctx, &databricks.ListRunsInput{
				JobId:         jobId,
				CompletedOnly: true,
			})
			if err != nil {
				errors = multierror.Append(errors, err)
			}

			for _, run := range history.Runs {
				if run.State.ResultState != nil && *run.State.ResultState == databricks.RunResultStateSuccess {
					if run.EndTime > mostRecent {
						mostRecent = run.EndTime
					}
				}
			}

			if mostRecent == -1 {
				// We'd like the time_since_last_success metric to always be populated
				// If we started with no mostRecent successes, and after checking again
				// with the ListRuns call there's no successes, we're going to fall
				// back to the earliest success associated with the job
				// which is when it was created.
				slog.Warnw(ctx, "no successful runs ever for this job", "job_id", jobId, "database", mj.database, "table", mj.table, "schedule", mj.schedule)
				if len(history.Runs) > 0 {
					// On-call requested that only jobs with runs page since old test jobs
					// were creating lots of high urgency noise - separate work needs to be
					// planned on to separate out all failures from no runs in the datadog
					// monitors.
					mostRecent = jobsById[jobId].CreatedTime
				}
			}

			if mostRecent == -1 {
				// If for some reason even after assigning CreatedTime there's no
				// available time to associate with the job, then log it.
				slog.Warnw(ctx, "no successful runs ever for this job and job's CreatedTime not available", "job_id", jobId, "database", mj.database, "table", mj.table, "schedule", mj.schedule)
				continue
			}
		}

		diff := w.clock.Now().Sub(samtime.MsToTime(mostRecent)).Milliseconds()
		metrics = append(
			metrics,
			datadoghttp.GaugeMetric(w.clock.Now(), metricPrefix+"databricks.jobs.time_since_last_success", float64(diff), tags),
		)

		// Due to how datadog SLOs work, we're not able to get group-level SLOs on a metric monitor. So if we made an
		// SLO based on time since last success, we'd only get overall rather than per-group.
		// However, if we do a "good events" vs "bad events" monitor, it seems to break down by group. So we'll log times
		// when we meet, and times when we break the SLO. And then use that kind of SLO.
		if mj.sloTarget != 0 {
			sloHoursTag := fmt.Sprintf("slo_hours:%d", mj.sloTarget)
			sloMetTag := "slo_met:false"
			if samtime.TimeToMs(w.clock.Now())-samtime.HoursToMs(mj.sloTarget) < mostRecent {
				sloMetTag = "slo_met:true"
			}
			metrics = append(
				metrics,
				datadoghttp.CounterMetric(w.clock.Now(), metricPrefix+"databricks.jobs.slo_counter", 1.0, append(tags, sloMetTag, sloHoursTag), 1.0),
			)
		}
	}

	// For all our managed jobs, emit metrics around their thresholds
	for id, mj := range managedJobsById {
		tags := w.tagsForManagedJob(id, mj)

		if mj.sloTarget != 0 {
			metrics = append(
				metrics,
				datadoghttp.GaugeMetric(w.clock.Now(), metricPrefix+"databricks.jobs.slo_target", float64(mj.sloTarget), tags),
			)
		}
		if mj.lowUrgencyThreshold != 0 {
			metrics = append(
				metrics,
				datadoghttp.GaugeMetric(w.clock.Now(), metricPrefix+"databricks.jobs.low_urgency_threshold", float64(mj.lowUrgencyThreshold), tags),
			)
		}
		if mj.businessHoursThreshold != 0 {
			metrics = append(
				metrics,
				datadoghttp.GaugeMetric(w.clock.Now(), metricPrefix+"databricks.jobs.business_hours_threshold", float64(mj.businessHoursThreshold), tags),
			)
		}
		if mj.highUrgencyThreshold != 0 {
			metrics = append(
				metrics,
				datadoghttp.GaugeMetric(w.clock.Now(), metricPrefix+"databricks.jobs.high_urgency_threshold", float64(mj.highUrgencyThreshold), tags),
			)
		}
	}

	return metrics, errors
}

// parseCustomDatapipelineTags parses the custom tags from the description field of a serverless datapipeline run.
// The description field is a JSON string that contains the custom tags.
func (w *Worker) parseCustomDatapipelineTags(jsonStr string) (map[string]string, error) {
	customTags := make(map[string]string)

	// Parse JSON into a temporary map that can handle different value types
	var tempMap map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &tempMap); err != nil {
		return nil, oops.Wrapf(err, "failed to unmarshal custom tags from description for serverless datapipeline run")
	}

	// Convert all values to strings
	for key, value := range tempMap {
		customTags[key] = fmt.Sprintf("%v", value)

	}

	return customTags, nil
}

func (w *Worker) fetchDatapipelinesMetrics(ctx context.Context) ([]datadog.Metric, error) {
	var metrics []datadog.Metric
	var errors error
	baseTags := []string{
		fmt.Sprintf("region:%s", w.region),
	}

	// Fetches all runs with SUBMIT_RUN run type in the last 24 hours.
	runs, err := w.getAllRuns(ctx, DatapipelinesRunsLookbackHours, databricks.RunTypeSubmitRun)
	if err != nil {
		return nil, oops.Wrapf(err, "failed getting SUBMIT_RUN runs")
	}

	for _, run := range runs {

		// Datapipelines jobs only have 1 task; if theres more, this can't be a datapipelines run
		// and we should skip it.
		if len(run.Tasks) != 1 {
			continue
		}
		task := run.Tasks[0]
		if task.NewCluster == nil && !strings.Contains(task.Description, "samsara:pooled-job:dataplatform-job-type") {
			// This is to primarily exclude dagster runs as their runTask details do not
			// contain ClusterSpecs. Also ensure they are not serverless datapipelines runs with custom tags in the description field.
			continue
		}

		var customTags map[string]string
		serverlessJob := "false"
		if task.NewCluster != nil {
			customTags = task.NewCluster.CustomTags
		} else {
			// Serverless datapipelines runs have their custom tags in the description field.
			customTags, err = w.parseCustomDatapipelineTags(task.Description)
			if err != nil {
				errors = multierror.Append(errors, oops.Wrapf(err, "failed to unmarshal custom tags from description for serverless datapipeline run for run id: %d", run.RunId))
				continue
			}
			serverlessJob = "true"
		}

		jobType := customTags["samsara:pooled-job:dataplatform-job-type"]
		if jobType != "data_pipelines_sql_transformation" && jobType != "data_pipelines_sqlite_node" {
			// Skip processing runs not tagged as datapipelines nodes.
			continue
		}

		nodeName := customTags["samsara:pipeline-node-name"]
		if nodeName == "" {
			errors = multierror.Append(errors, oops.Errorf("pipeline-node-name not found in cluster tags"))
		}
		status := strings.ToLower(string(*run.State.ResultState))
		tags := append([]string{
			fmt.Sprintf("node:%s", nodeName),
			fmt.Sprintf("status:%s", status),
		}, baseTags...)

		if status == "failed" {
			// get Run Output for failed runs to get error message for classification.
			runOutput, err := w.databricksClient.GetRunOutput(
				ctx,
				&databricks.GetRunOutputInput{
					RunId: run.RunId,
				},
			)
			if err != nil {
				errors = multierror.Append(errors, oops.Wrapf(err, "get run output"))
			}

			pipelineErrors := datapipelineerrors.GetErrorClassification(runOutput.Error)
			tags = append(tags, []string{
				fmt.Sprintf("cause:%s", pipelineErrors.ErrorCategory),
				fmt.Sprintf("error:%s", pipelineErrors.ErrorType),
			}...)
		}

		// Add serverless tag to tags.
		tags = append(tags, fmt.Sprintf("serverless:%s", serverlessJob))

		// Append prefix `dev.` to metrics to allow local testing and not overwrite prod metrics.
		metricPrefix := ""
		if samsaraaws.IsRunningInLocalDev() {
			metricPrefix = "dev."
		}
		metrics = append(
			metrics,
			datadoghttp.CountMetric(
				samtime.MsToTime(run.EndTime),
				metricPrefix+"databricks.datapipelines.run.finish",
				1,
				tags,
			),
		)

	}

	return metrics, errors
}

func (w *Worker) fetchInstancePoolMetrics(ctx context.Context) ([]datadog.Metric, error) {
	var metrics []datadog.Metric
	baseTags := []string{
		fmt.Sprintf("region:%s", w.region),
	}
	metricPrefix := ""
	if samsaraaws.IsRunningInLocalDev() {
		metricPrefix = "dev."
	}

	pools, err := w.databricksClient.ListInstancePools(ctx)
	if err != nil {
		return nil, oops.Wrapf(err, "list instance pools")
	}

	for _, pool := range pools.InstancePools {
		tags := append(baseTags, fmt.Sprintf("pool_name:%s", monitoring.SanitizeDatadogMetricName(pool.InstancePoolName)))
		metrics = append(
			metrics,
			datadoghttp.GaugeMetric(
				w.clock.Now(),
				metricPrefix+"databricks.instance_pools.min_idle_count",
				float64(pool.MinIdleInstances),
				tags,
			),
			datadoghttp.GaugeMetric(
				w.clock.Now(),
				metricPrefix+"databricks.instance_pools.idle_count",
				float64(pool.Stats.IdleCount),
				tags,
			),
			datadoghttp.GaugeMetric(
				w.clock.Now(),
				metricPrefix+"databricks.instance_pools.pending_idle_count",
				float64(pool.Stats.PendingIdleCount),
				tags,
			),
			datadoghttp.GaugeMetric(
				w.clock.Now(),
				metricPrefix+"databricks.instance_pools.used_count",
				float64(pool.Stats.UsedCount),
				tags,
			),
			datadoghttp.GaugeMetric(
				w.clock.Now(),
				metricPrefix+"databricks.instance_pools.pending_used_count",
				float64(pool.Stats.PendingUsedCount),
				tags,
			),
		)
	}

	return metrics, nil
}

func (w *Worker) getAllRuns(ctx context.Context, hoursLookback int64, runType databricks.RunType) ([]*databricks.Run, error) {
	currentTime := samtime.TimeToMs(w.clock.Now())
	startTime := currentTime - (hoursLookback * 60 * 60 * 1000)
	output, err := w.databricksClient.ListRuns(ctx, &databricks.ListRunsInput{
		// List only jobs that have completed, and expandTasks receive task information so we can emit
		// task-level metrics alongside job-level ones.
		CompletedOnly: true,
		StartTimeFrom: int(startTime),
		ExpandTasks:   true,
		RunType:       string(runType),
	})
	if err != nil {
		return nil, oops.Wrapf(err, "failed to fetch runs")
	}

	slog.Infow(ctx, fmt.Sprintf("Finished fetching runs, got: %d\n", len(output.Runs)))
	return output.Runs, nil
}

func (w *Worker) tagsForManagedJob(jobId int64, mj managedJob) []string {
	return []string{
		fmt.Sprintf("job_id:%d", jobId),
		fmt.Sprintf("job_name:%s", mj.jobName),
		fmt.Sprintf("database:%s", mj.database),
		fmt.Sprintf("table:%s", mj.table),
		fmt.Sprintf("schedule:%s", mj.schedule),
		fmt.Sprintf("region:%s", w.region),
		fmt.Sprintf("%s:%s", dataplatformconsts.DATAPLAT_JOBTYPE_TAG, mj.jobType),
	}
}

// In serverless jobs, the tags are set on the job settings, not the cluster.
func getSamsaraJobTags(ctx context.Context, jobTags map[string]string) []string {
	var tags []string

	if jobTags == nil || len(jobTags) == 0 {
		return tags
	}

	team := "unknown"
	if val, ok := jobTags["samsara:team"]; ok {
		team = val
	}

	tags = append(tags, "team:"+team)

	for key, val := range jobTags {

		if key == "samsara:team" {
			// We already added this tag above.
			continue
		}

		if strings.HasPrefix(key, "samsara:") {
			key = strings.ReplaceAll(key, "samsara:", "")
		}

		tags = append(tags, key+":"+val)
	}

	return tags
}

// TODO: we don't extract these tags from "existing" clusters. I don't think this
// is a common use-case so it should probably be okay.
func getSamsaraTags(ctx context.Context, poolsById map[string]databricks.InstancePoolAndStats, clusterSpec *databricks.ClusterSpec, jobTags map[string]string) []string {
	var tags []string

	if clusterSpec == nil {
		// if we don't have a cluster spec, we can just use the job tags.
		tags = getSamsaraJobTags(ctx, jobTags)
		// Add serverless tag to base tags. Since clusterSpec is nil, this is always true.
		tags = append(tags, "serverless:true")
		return tags
	}

	// For the team tag, we want to prioritize `samsara:{job,pooled_job}:team` but use
	// the always(?) set `samsara:team` tag otherwise
	team := "unknown"
	if val, ok := clusterSpec.CustomTags["samsara:team"]; ok {
		team = val
	}

	for key, val := range clusterSpec.CustomTags {
		if key == "samsara:job:team" || key == "samsara:pooled-job:team" {
			team = val
		} else if strings.HasPrefix(key, "samsara:job:") {
			tag := strings.ReplaceAll(key, "samsara:job:", "") + ":" + val
			tags = append(tags, tag)
		} else if strings.HasPrefix(key, "samsara:pooled-job:") {
			tag := strings.ReplaceAll(key, "samsara:pooled-job:", "") + ":" + val
			tags = append(tags, tag)
		} else if strings.HasPrefix(key, "samsara:") && key != "samsara:team" {
			tag := strings.ReplaceAll(key, "samsara:", "") + ":" + val
			tags = append(tags, tag)
		}
	}
	tags = append(tags, "team:"+team)

	// Try to get the zone_id from the aws_attributes. For pooled jobs, we'll have to look
	// at the instance pools.
	zone_id := "unknown"
	if clusterSpec.AwsAttributes != nil && clusterSpec.AwsAttributes.ZoneId != "" {
		zone_id = clusterSpec.AwsAttributes.ZoneId
	} else if clusterSpec.InstancePoolId != "" {
		// It should be sufficient to just look by the instance pool id, since we will place drivers/workers in the same AZ
		if pool, ok := poolsById[clusterSpec.InstancePoolId]; ok {
			if pool.AwsAttributes != nil && pool.AwsAttributes.ZoneId != "" {
				zone_id = pool.AwsAttributes.ZoneId
			} else {
				slog.Warnw(ctx, "couldn't figure out zone id for pool "+clusterSpec.InstancePoolId)
			}
		} else {
			slog.Warnw(ctx, "couldn't find pool with id "+clusterSpec.InstancePoolId)
		}
	}

	tags = append(tags, "availability_zone:"+zone_id)

	// Add serverless tag to base tags. Since we are using classic clusters, this is always false.
	tags = append(tags, "serverless:false")
	return tags
}

// We need to limit the total tag length to 200 chars.
// Let's standardize the name by
// - remove dissalowed chars (see regex)
// - replace spaces with `_`
// - hashtruncate to 190 chars
// So that with the `job_name:` prefix it comes to under 200.
// This may not all be strictly necessary from datadog, but seems nice for cleanliness,
// since names can be arbitrary.
func cleanName(name string) string {
	return util.HashTruncate(
		strings.ToLower(
			spaceRegex.ReplaceAllString(
				strings.TrimSpace(disallowedCharsRegex.ReplaceAllString(name, "")),
				"_",
			),
		),
		190, 5, "",
	)
}

func parseIntOrZero(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(New)
}
