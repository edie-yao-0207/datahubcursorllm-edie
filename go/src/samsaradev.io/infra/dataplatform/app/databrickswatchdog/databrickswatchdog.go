package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/benbjohnson/clock"
	"github.com/cznic/mathutil"
	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"

	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/samtime"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/serviceorchestration/leaderelection"
)

type Worker struct {
	databricksClient databricks.API
	clock            clock.Clock
}

const MINUTES_BETWEEN_RUNS = 30

const LONG_RUNNING_JOB_THRESHOLD = int(samtime.MillisecondsInHour)

const RETRIABLE_JOBS_LIMIT = 50

type JobClusterMetadata struct {
	Job       databricks.Job
	ClusterId string
}

type JobRunsMetadata struct {
	Job         databricks.Job
	isProdTable bool
	Runs        []*databricks.Run
}

func New(
	lc fx.Lifecycle,
	databricksClient databricks.API,
	clock clock.Clock,
	leaderElectionHelper leaderelection.WaitForLeaderLifecycleHookHelper,
) (*Worker, error) {

	w := &Worker{
		databricksClient: databricksClient,
		clock:            clock,
	}

	lc.Append(leaderElectionHelper.RunAfterLeaderElectionLifecycleHook(func(ctx context.Context) {
		if err := w.fetchDatabricksJobsContinuously(ctx); oops.Cause(err) != context.Canceled {
			slog.Fatalw(ctx, "unexpected error running databrickswatchdog", "error", err)
		}
	}))

	return w, nil
}

func (w *Worker) fetchDatabricksJobsContinuously(ctx context.Context) error {
	errorCount := 0
	for {
		// skiplint: +loopedexpensivecall
		mergeJobs, err := w.getDataPlatformMergeJobs(ctx)
		if err != nil {
			return oops.Wrapf(err, "failed to get data platform merge jobs")
		}
		// Upsize long-running merge jobs.
		if err := w.upsizeLongRunningMergeJobClusters(ctx, mergeJobs); err != nil {
			monitoring.AggregatedDatadog.Incr("databrickswatchdog.run", "success:false")
			errorCount += 1
			if errorCount >= 3 {
				return oops.Wrapf(err, "databrickswatchdog failed to upsize long running merge job clusters 3 times in a row.")
			} else {
				slog.Warnw(ctx, fmt.Sprintf("databrickswatchdog upsizeLongRunningMergeJobClusters failed, but will retry again. errorcount: %d. error: %v", errorCount, err))
			}
		}
		// Retry failed merge jobs.
		if err := w.retryFailedMergeJobs(ctx, mergeJobs); err != nil {
			monitoring.AggregatedDatadog.Incr("databrickswatchdog.run", "success:false")
			errorCount += 1
			if errorCount >= 3 {
				return oops.Wrapf(err, "databrickswatchdog failed to retry failed merge jobs 3 times in a row.")
			} else {
				slog.Warnw(ctx, fmt.Sprintf("databrickswatchdog retryFailedMergeJobs failed, but will retry again. errorcount: %d. error: %v", errorCount, err))
			}
		}
		if errorCount == 0 {
			monitoring.AggregatedDatadog.Incr("databrickswatchdog.run", "success:true")
		}
		// Sleep in between runs to space out calls to the DBX API to avoid any rate
		// limit issues or infinite looping if the API is down.
		slog.Infow(ctx, fmt.Sprintf("Sleeping %d minutes between runs", MINUTES_BETWEEN_RUNS))
		if err := aws.SleepWithContext(ctx, time.Minute*MINUTES_BETWEEN_RUNS); err != nil {
			return oops.Wrapf(err, "sleeping between fetching databricks jobs")
		}
	}
}

func getCustomTagValue(tags map[string]string, tagName string) string {
	for key, val := range tags {
		if strings.Contains(key, tagName) {
			return val
		}
	}
	return ""
}

func containsDataPlatformMergeJobTag(tags map[string]string) bool {
	dataPlatformJobType := getCustomTagValue(tags, dataplatformconsts.DATAPLAT_JOBTYPE_TAG)
	mergeJobTagValues := []dataplatformconsts.DataPlatformJobType{
		dataplatformconsts.KinesisStatsDeltaLakeIngestionMerge,
		dataplatformconsts.RdsDeltaLakeIngestionMerge,
		dataplatformconsts.DynamoDbDeltaLakeIngestionMerge,
		dataplatformconsts.KinesisStatsBigStatsDeltaLakeIngestionMerge,
		dataplatformconsts.ReportSqliteAggregator,
	}
	for _, k := range mergeJobTagValues {
		tag := string(k)
		if strings.Contains(dataPlatformJobType, tag) {
			return true
		}
	}

	return false
}

// upsizeAutoscaleConfiguration will quadruple the ClusterAutoScale's current
// max worker configuration (capped at 128 workers).
func upsizeAutoscaleConfiguration(autoscaleConfig databricks.ClusterAutoScale) databricks.ClusterAutoScale {
	return databricks.ClusterAutoScale{
		MinWorkers: autoscaleConfig.MinWorkers,
		MaxWorkers: mathutil.Min(autoscaleConfig.MaxWorkers*4, 128),
	}
}

// getUpsizeClusterDurationThreshold calculates the threshold duration in
// milliseconds for upsizing a cluster based on the specified SLO target. The
// function searches for the "samsara:pooled-job:slo-target" tag in the provided
// tags map, which should represent the SLO duration in hours. The threshold is
// determined by dividing the SLO target duration by 10 and rounding up to the
// nearest integer, ensuring a minimum return value of 1hr. This means that any
// SLO target duration of less than 10 hours will result in a threshold of 1hr.
// The function returns this calculated threshold as milliseconds. If the SLO
// target duration is not found or cannot be converted to an integer, an error
// is returned.
func getUpsizeClusterDurationThreshold(jobName string, tags map[string]string) (int, error) {
	// Retrieve the SLO target hours from custom tags.
	sloTargetHours := getCustomTagValue(tags, dataplatformconsts.SLO_TARGET_TAG)
	if sloTargetHours == "" {
		// Default to 1 hour if the slo-target tag is not set.
		monitoring.AggregatedDatadog.Incr("databrickswatchdog.missing_slo_target_tag_value", fmt.Sprintf("job_name:%s", jobName))
		return samtime.MillisecondsInHour, nil
	}

	// Convert the slo-target tag value from a string to an integer.
	hours, err := strconv.Atoi(sloTargetHours)
	if err != nil {
		return 0, oops.Wrapf(err, "%s", fmt.Sprintf("error converting slo-target hours value: %s", sloTargetHours))
	}

	// Calculate the threshold in milliseconds (max 1/10 of hours, minimum of 1 hour).
	return int(math.Round(float64(mathutil.Max(hours/10, 1))) * samtime.MillisecondsInHour), nil
}

// isClusterSizeUnchanged checks if the current cluster size settings match the
// job setting cluster size settings. This function is used to determine if a
// cluster has undergone any upscaling or not.
func isClusterSizeUnchanged(jobSettingCluster, currentCluster *databricks.ClusterAutoScale) bool {
	return currentCluster.MinWorkers == jobSettingCluster.MinWorkers &&
		currentCluster.MaxWorkers == jobSettingCluster.MaxWorkers
}

// getDataPlatformMergeJobs will get all KS, RDS, and DynamoDb merge jobs.
func (w *Worker) getDataPlatformMergeJobs(ctx context.Context) ([]databricks.Job, error) {
	output, err := w.databricksClient.ListJobs(ctx, &databricks.ListJobsInput{
		ExpandTasks: true,
	})
	if err != nil {
		errorMessage := "error listing jobs"
		if output != nil {
			errorMessage = fmt.Sprintf("%s, %s: %s", errorMessage, output.ErrorCode, output.Message)
		}
		return nil, oops.Wrapf(err, "%s", errorMessage)
	}
	databricksMergeJobs := make([]databricks.Job, 0, len(output.Jobs))
	for _, job := range output.Jobs {
		if len(job.JobSettings.JobClusters) > 0 {
			if containsDataPlatformMergeJobTag(job.JobSettings.JobClusters[0].NewCluster.CustomTags) {
				databricksMergeJobs = append(databricksMergeJobs, job)
			}
		}
	}
	return databricksMergeJobs, nil
}

func (w *Worker) upsizeLongRunningMergeJobClusters(ctx context.Context, mergeJobs []databricks.Job) error {
	// Step 1: Get all long running merge jobs (ie active runs that have been
	// running for at least an hour).
	clusterInfoForLongRunningMergeJobs := make([]JobClusterMetadata, 0, len(mergeJobs))
	for _, job := range mergeJobs {
		output, err := w.databricksClient.ListRuns(ctx, &databricks.ListRunsInput{
			JobId:       job.JobId,
			ActiveOnly:  true,
			ExpandTasks: true,
		})
		if err != nil {
			errorMessage := "error listing runs"
			if output != nil {
				errorMessage = fmt.Sprintf("%s, %s: %s", errorMessage, output.ErrorCode, output.Message)
			}
			return oops.Wrapf(err, "%s", errorMessage)
		}
		// If there are multiple active runs, there may be something weird going on.
		// We shouldn't block resizing the clusters, but we'll log it so we can take
		// action on it.
		if len(output.Runs) > 1 {
			slog.Warnw(ctx, "job has concurrent runs", "jobId", job.JobId, "numRuns", len(output.Runs))
		}
		for _, run := range output.Runs {
			threshold, err := getUpsizeClusterDurationThreshold(job.JobSettings.Name, job.JobSettings.JobClusters[0].NewCluster.CustomTags)
			if err != nil {
				return oops.Wrapf(err, "error getting duration threshold")
			}
			currentTimeMs := samtime.TimeToMs(w.clock.Now())
			if int(currentTimeMs-run.StartTime) >= threshold {
				if len(run.Tasks) == 1 {
					if run.Tasks[0].ClusterInstance == nil {
						slog.Warnw(ctx, "cluster instance is nil", "jobId", job.JobId, "jobName", job.JobSettings.Name, "State", run.State.LifeCycleState)
						continue
					}
					clusterId := run.Tasks[0].ClusterInstance.ClusterId
					clusterInfoForLongRunningMergeJobs = append(clusterInfoForLongRunningMergeJobs, JobClusterMetadata{
						Job:       job,
						ClusterId: clusterId,
					})
				} else {
					slog.Warnw(ctx, "unexpected number of tasks", "jobId", job.JobId, "numTasks", len(run.Tasks))
				}
			}
		}
	}

	// Step 2: Upsize clusters for long running merge jobs.
	for _, job := range clusterInfoForLongRunningMergeJobs {
		cluster, err := w.databricksClient.GetCluster(ctx, &databricks.GetClusterInput{
			ClusterId: job.ClusterId,
		})
		if err != nil {
			return oops.Wrapf(err, "error getting cluster")
		}
		jobSettingCluster := job.Job.JobSettings.JobClusters[0].NewCluster.AutoScale
		if jobSettingCluster == nil {
			slog.Warnw(ctx, "job setting cluster autoscale configuration is nil", "JobId", job.Job.JobId, "JobName", job.Job.JobSettings.Name)
			continue
		}
		// Only upsize clusters if they have not yet been upsized.
		if isClusterSizeUnchanged(jobSettingCluster, cluster.ClusterSpec.AutoScale) {
			if output, err := w.databricksClient.ResizeCluster(ctx, &databricks.ResizeClusterInput{
				ClusterId: job.ClusterId,
				Autoscale: upsizeAutoscaleConfiguration(*jobSettingCluster),
			}); err != nil {
				monitoring.AggregatedDatadog.Incr("databrickswatchdog.resize_cluster", "success:false", fmt.Sprintf("job_name:%s", job.Job.JobSettings.Name))
				errorMessage := fmt.Sprintf("error resizing cluster for %s", job.Job.JobSettings.Name)
				if output != nil {
					errorMessage = fmt.Sprintf("%s, %s: %s", errorMessage, output.ErrorCode, output.Message)
				}
				return oops.Wrapf(err, "%s", errorMessage)
			} else {
				slog.Infow(ctx, fmt.Sprintf("resized cluster for %s", job.Job.JobSettings.Name))
				monitoring.AggregatedDatadog.Incr("databrickswatchdog.resize_cluster", "success:true", fmt.Sprintf("job_name:%s", job.Job.JobSettings.Name))
			}
		}
	}
	return nil
}

// isErrorRetriable checks errMessage against a set of predefined errors that we
// consider retriable (e.g. AWS insufficient capacity) and returns true if it
// matches any one of those.
func isErrorRetriable(errMessage string) bool {
	retriableErrors := []string{
		// Example error message:
		// Task regulation_mode_driver_time_ranges-v0 failed with message: Cluster
		// 1201-152702-6zjz069j was terminated while waiting on it to be ready:
		// Cluster 1201-152702-6zjz069j is in unexpected state Terminated:
		// AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE(CLIENT_ERROR):
		// aws_api_error_code:InsufficientInstanceCapacity,aws_error_message:There
		// is no Spot capacity available that matches your request.. This caused all
		// downstream tasks to get skipped.
		"AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE",
		// org.apache.spark.SparkException: Job aborted due to stage failure: Task
		// 13 in stage 1549.0 failed 4 times, most recent failure: Lost task 13.3 in
		// stage 1549.0 (TID 5699179) (10.48.45.159 executor 992):
		// org.apache.spark.SparkException:  Checkpoint block rdd_4255_138 not
		// found! Either the executor that originally checkpointed this partition is
		// no longer alive, or the original RDD is unpersisted. If this problem
		// persists, you may consider using `rdd.checkpoint()` instead, which is
		// slower than local checkpointing but more fault-tolerant.
		"Either the executor that originally checkpointed this partition is no longer alive, or the original RDD is unpersisted. If this problem persists, you may consider using `rdd.checkpoint()` instead, which is slower than local checkpointing but more fault-tolerant.",
		// org.apache.spark.SparkException: Job aborted due to stage failure: Task 2601
		// in stage 583.0 failed 4 times, most recent failure: Lost task 2601.3 in stage
		// 583.0 (TID 40393) (10.48.71.186 executor 53): org.apache.spark.api.python.PythonException:
		// 'sqlite3.IntegrityError: UNIQUE constraint failed: speeding_report_v3.month,
		// speeding_report_v3.org_id, speeding_report_v3.duration_ms, speeding_report_v3.interval_start,
		// speeding_report_v3.unit, speeding_report_v3.object_type, speeding_report_v3.object_id'.
		"sqlite3.IntegrityError: UNIQUE constraint failed:",
	}
	for _, err := range retriableErrors {
		if strings.Contains(errMessage, err) {
			return true
		}
	}
	return false
}

// isJobEligibleForRetry determines if a job is eligible for a retry based on
// the result state of its most recent run result state and state message.
// A job is considered eligible for retry if the most recent run failed
// with a retriable error.
func (w *Worker) isJobEligibleForRetry(ctx context.Context, recentRun *databricks.Run) (bool, error) {

	// If the recent run result state is nil we don't need to retry
	if recentRun.State.ResultState == nil {
		slog.Warnw(ctx, "recent run result state is nil for this run", "jobId", recentRun.JobId, "runId", recentRun.RunId, "jobRunName", recentRun.RunName)
		return false, nil
	}

	if *recentRun.State.ResultState == databricks.RunResultStateFailed && isErrorRetriable(recentRun.State.StateMessage) {
		return true, nil
	}
	// If an error message is really long, we need to use the GetRunOutput API to
	// get the full stacktrace.
	if *recentRun.State.ResultState == databricks.RunResultStateFailed {
		runId := recentRun.Tasks[0].RunId
		output, err := w.databricksClient.GetRunOutput(ctx, &databricks.GetRunOutputInput{
			RunId: runId,
		})
		if err != nil {
			return false, oops.Wrapf(err, "%s", fmt.Sprintf("error getting run output for runId %d, %s: %s", runId, output.ErrorCode, output.Message))
		}
		// The GetRunOutput API returns a stack trace with this formatting:
		// Either the executor\nthat originally checkpointed this partition is no
		// longer alive, or the original RDD is\nunpersisted. If this problem
		// persists, you may consider using `rdd.checkpoint()`\ninstead, which is
		// slower than local checkpointing but more fault-tolerant.\n
		// So we need to replace all newline characters with spaces.
		sanitizedErrorTrace := strings.ReplaceAll(output.ErrorTrace, "\n", " ")
		return isErrorRetriable(sanitizedErrorTrace), nil
	}
	return false, nil
}

// prioritizeProductionJobs sorts a slice of JobRunsMetadata such that
// production jobs are prioritized.
func prioritizeProductionJobs(mergeJobs []JobRunsMetadata) []JobRunsMetadata {
	sort.Slice(mergeJobs, func(i, j int) bool {
		return mergeJobs[i].isProdTable && !mergeJobs[j].isProdTable
	})
	return mergeJobs
}

// retryFailedMergeJobs identifies KS, RDS, and DynamoDB jobs that failed in
// their most recent runs or have ongoing runs with previous failures, and
// triggers a retry for these jobs. The function retrieves a list of all merge
// jobs, fetches the most recent three runs for each job, determines which jobs
// should be retried based on the status of these runs, and finally triggers a
// retry for each job identified as needing a retry.
func (w *Worker) retryFailedMergeJobs(ctx context.Context, mergeJobs []databricks.Job) error {
	// Step 1: Find all merge jobs that failed due to retriable errors.
	retriableMergeJobs := make([]JobRunsMetadata, 0, len(mergeJobs))
	for _, job := range mergeJobs {
		output, err := w.databricksClient.ListRuns(ctx, &databricks.ListRunsInput{
			JobId:       job.JobId,
			ExpandTasks: true,
			Limit:       2,
		})
		if err != nil {
			return oops.Wrapf(err, "%s", fmt.Sprintf("error listing runs, %s: %s", output.ErrorCode, output.Message))
		}
		// If there are no runs OR there is an active run, then we don't need to retry the job.
		if len(output.Runs) == 0 || (len(output.Runs) > 0 &&
			output.Runs[0].State.LifeCycleState == databricks.RunLifeCycleStateRunning) {
			continue
		}
		isProdTable := getCustomTagValue(job.JobSettings.JobClusters[0].NewCluster.CustomTags, dataplatformconsts.PRODUCTION_TAG) == "true"
		isEligibleForRetry, err := w.isJobEligibleForRetry(ctx, output.Runs[0])
		if err != nil {
			return oops.Wrapf(err, "error checking if job is eligible for retry")
		}
		if isEligibleForRetry {
			retriableMergeJobs = append(retriableMergeJobs, JobRunsMetadata{
				Job:         job,
				isProdTable: isProdTable,
				Runs:        output.Runs,
			})
		}
	}
	// Prioritize retrying production jobs.
	retriableMergeJobs = prioritizeProductionJobs(retriableMergeJobs)
	jobsRetried := 0
	// Step 2: Retry merge jobs that failed due to retriable errors.
	for _, job := range retriableMergeJobs {
		// If the number of jobs we're retrying is more than the allowable limit,
		// don't attempt to retry any more jobs.
		if jobsRetried > RETRIABLE_JOBS_LIMIT {
			slog.Warnw(ctx, "reached retriable jobs limit", "num_jobs_not_retried", len(retriableMergeJobs)-jobsRetried)
			monitoring.AggregatedDatadog.Incr("databrickswatchdog.retriable_job_limit_reached")
			return nil
		}
		if output, err := w.databricksClient.RunNow(ctx, &databricks.RunNowInput{
			JobId: job.Job.JobId,
		}); err != nil {
			monitoring.AggregatedDatadog.Incr("databrickswatchdog.retry_job", "success:false", fmt.Sprintf("job_name:%s", job.Job.JobSettings.Name))
			return oops.Wrapf(err, "%s", fmt.Sprintf("error triggering new run for %s, %s: %s", job.Job.JobSettings.Name, output.ErrorCode, output.Message))
		} else {
			monitoring.AggregatedDatadog.Incr("databrickswatchdog.retry_job", "success:true", fmt.Sprintf("job_name:%s", job.Job.JobSettings.Name))
		}
		jobsRetried++
	}
	return nil
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(New)
}
