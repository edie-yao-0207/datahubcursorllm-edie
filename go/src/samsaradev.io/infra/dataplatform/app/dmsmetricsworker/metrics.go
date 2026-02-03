package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice/databasemigrationserviceiface"
	"github.com/cznic/mathutil"
	"github.com/hashicorp/go-multierror"
	"github.com/samsarahq/go/oops"
	"github.com/zorkian/go-datadog-api"
	"go.uber.org/fx"
	"golang.org/x/time/rate"

	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/samtime"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/fxregistry"
	"samsaradev.io/infra/monitoring/datadoghttp"
	"samsaradev.io/infra/monitoring/datadogutils"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
	"samsaradev.io/infra/serviceorchestration/leaderelection"
)

type Worker struct {
	region        string
	dmsClient     databasemigrationserviceiface.DatabaseMigrationServiceAPI
	datadogClient *datadog.Client
}

func New(
	lc fx.Lifecycle,
	datadogClient *datadog.Client,
	leaderElectionHelper leaderelection.WaitForLeaderLifecycleHookHelper,
) *Worker {
	sess := awssessions.NewInstrumentedAWSSession()
	region := aws.StringValue(sess.Config.Region)
	dmsClient := databasemigrationservice.New(sess)

	w := &Worker{
		region:        region,
		dmsClient:     dmsClient,
		datadogClient: datadogClient,
	}

	lc.Append(leaderElectionHelper.RunAfterLeaderElectionLifecycleHook(func(ctx context.Context) {
		if err := w.fetchMetricsContinuously(ctx); oops.Cause(err) != context.Canceled {
			slog.Fatalw(ctx, "unexpected error running dmsmetricsworker", "error", err)
		}
	}))

	return w
}

func (w *Worker) fetchMetricsContinuously(ctx context.Context) error {
	errorCount := 0
	for {
		if err := w.collectAndPostMetrics(ctx); err != nil {
			monitoring.AggregatedDatadog.Incr("dmsmetricsworker.run", "success:false")
			errorCount += 1
			if errorCount >= 3 {
				return oops.Wrapf(err, "failed to post metrics 3 times in a row.")
			} else {
				slog.Warnw(ctx, fmt.Sprintf("failed to collect and post metrics, but will retry again. errorcount: %d. error: %v", errorCount, err))
			}
		} else {
			monitoring.AggregatedDatadog.Incr("dmsmetricsworker.run", "success:true")
		}

		// Sleep in between runs to space out calls to the DMS API to avoid any rate limit issues or
		// infinite looping if the API is down.
		slog.Infow(ctx, "Sleeping 5 minutes between runs")
		if err := aws.SleepWithContext(ctx, time.Minute*5); err != nil {
			return oops.Wrapf(err, "sleeping between fetching metrics")
		}
	}
}

func (w *Worker) collectAndPostMetrics(ctx context.Context) error {
	// Log how long each iteration took
	start := samtime.TimeNowInMs()
	defer func() {
		elapsed := samtime.TimeNowInMs() - start
		slog.Infow(ctx, fmt.Sprintf("poll took %.2f seconds", float64(elapsed)/1000))
		monitoring.AggregatedDatadog.Gauge(float64(elapsed), "dmsmetricsworker.poll_duration")
	}()

	var collectedErrors *multierror.Error

	// Even if we get some errors, log whatever metrics we did get.
	metrics, err := w.fetchMetrics(ctx)
	if err != nil {
		slog.Errorw(ctx, oops.Wrapf(err, "fetch metrics"), nil)
		collectedErrors = multierror.Append(collectedErrors, err)
	}
	datadogutils.IncrResult(err, "dmsmetricsworker.fetch.count")
	slog.Infow(ctx, "fetched metric count", "count", len(metrics))

	// Limit metrics per POST to avoid hitting size limit.
	const pageSize = 1000
	for i := 0; i*pageSize < len(metrics); i++ {
		page := metrics[i*pageSize : mathutil.Min((i+1)*pageSize, len(metrics))]
		if err := w.datadogClient.PostMetrics(page); err != nil {
			slog.Errorw(ctx, oops.Wrapf(err, "post metrics"), nil)
			collectedErrors = multierror.Append(collectedErrors, err)
		}
		datadogutils.IncrResult(err, "dmsmetricsworker.post.count")
	}
	slog.Infow(ctx, "done posting metrics")

	return collectedErrors.ErrorOrNil()
}

func (w *Worker) fetchMetrics(ctx context.Context) ([]datadog.Metric, error) {
	var metrics []datadog.Metric
	now := time.Now()

	baseTags := []string{
		fmt.Sprintf("region:%s", w.region),
	}

	var tasks []*databasemigrationservice.ReplicationTask
	if err := w.dmsClient.DescribeReplicationTasksPagesWithContext(ctx,
		&databasemigrationservice.DescribeReplicationTasksInput{
			WithoutSettings: aws.Bool(true),
		},
		func(output *databasemigrationservice.DescribeReplicationTasksOutput, _ bool) bool {
			tasks = append(tasks, output.ReplicationTasks...)
			return true
		},
	); err != nil {
		return nil, oops.Wrapf(err, "describe replication tasks")
	}

	instances := make(map[string]*databasemigrationservice.ReplicationInstance)
	if err := w.dmsClient.DescribeReplicationInstancesPagesWithContext(ctx,
		&databasemigrationservice.DescribeReplicationInstancesInput{},
		func(output *databasemigrationservice.DescribeReplicationInstancesOutput, _ bool) bool {
			for _, instance := range output.ReplicationInstances {
				instances[aws.StringValue(instance.ReplicationInstanceArn)] = instance
			}
			return true
		},
	); err != nil {
		return nil, oops.Wrapf(err, "describe replication instances")
	}

	slog.Infow(ctx, "top-level dms metrics", "tasks", len(tasks), "instances", len(instances))
	metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.num_tasks", float64(len(tasks)), baseTags))
	metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.num_instances", float64(len(instances)), baseTags))

	var allErrors error

	// The DMS API rate limits work on a token bucket model where:
	// - max 200 tokens in the bucket
	// - bucket refreshes at 8 api calls per second.
	// This rate limit is applied to any api calls to dms, including usage of the AWS UI.
	// For now, we're allowing up to 5 requests per second to prevent interfering with other
	// use cases. It's not exact because i'm not sure how the `describeTableStatisticsWithPages`
	// does rate limiting, but given how low we're keeping it we think it should be safe.
	//
	// https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Limits.html
	limiter := rate.NewLimiter(rate.Limit(5), 1)
	for idx, task := range tasks {
		if idx%10 == 0 {
			slog.Infow(ctx, fmt.Sprintf("on task %d of %d, name: %s", idx, len(tasks), aws.StringValue(task.ReplicationTaskIdentifier)))
		}
		taskId := aws.StringValue(task.ReplicationTaskIdentifier)

		instance, ok := instances[aws.StringValue(task.ReplicationInstanceArn)]
		if !ok {
			allErrors = multierror.Append(allErrors, oops.Errorf("replication instance %s not found for task: %s", aws.StringValue(task.ReplicationInstanceArn), taskId))
			continue
		}
		instanceId := aws.StringValue(instance.ReplicationInstanceIdentifier)

		taskTags := append(append([]string(nil), baseTags...),
			fmt.Sprintf("replicationinstanceidentifier:%s", instanceId),
			// TODO: eventually, deprecate this typo tag here and in our dashboards.
			fmt.Sprintf("repilcationtaskidentifier:%s", taskId),
			fmt.Sprintf("replicationtaskidentifier:%s", taskId),
		)

		// Add AWS tags from DMS task to Datadog Metrics tag. This will help with targeted monitoring on the Datadog side because
		// there are multiple teams using DMS now. We use the tags on the Task because care about how the task is operating rather
		// than the instance the task is running on. This may change in the future.
		if err := limiter.Wait(ctx); err != nil {
			allErrors = multierror.Append(allErrors, oops.Wrapf(err, "exception from rate limiter"))
			continue
		}
		tags, err := w.dmsClient.ListTagsForResource(&databasemigrationservice.ListTagsForResourceInput{
			ResourceArn: task.ReplicationTaskArn,
		})
		if err != nil {
			allErrors = multierror.Append(allErrors, oops.Wrapf(err, "Error getting tags for dms instance %s", instanceId))
			continue
		}
		for _, tag := range tags.TagList {
			taskTags = append(taskTags, fmt.Sprintf("%s:%s", aws.StringValue(tag.Key), aws.StringValue(tag.Value)))
		}

		status := aws.StringValue(task.Status)
		running := float64(0)
		if status == "running" {
			running = 1
		}
		metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.task.running", running, taskTags))

		failed := float64(0)
		if status == "failed" {
			failed = 1
		}
		metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.task.failed", failed, taskTags))

		stopped := float64(0)
		if status == "stopped" {
			stopped = 1
		}

		metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.task.stopped", stopped, taskTags))

		created := float64(0)
		if status == "created" {
			created = 1
		}

		metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.task.created", created, taskTags))

		var tables []*databasemigrationservice.TableStatistics
		if err := limiter.Wait(ctx); err != nil {
			allErrors = multierror.Append(allErrors, oops.Wrapf(err, "exception from rate limiter"))
			continue
		}
		if err := w.dmsClient.DescribeTableStatisticsPagesWithContext(ctx,
			&databasemigrationservice.DescribeTableStatisticsInput{
				ReplicationTaskArn: task.ReplicationTaskArn,
			},
			func(output *databasemigrationservice.DescribeTableStatisticsOutput, _ bool) bool {
				tables = append(tables, output.TableStatistics...)
				return true
			},
		); err != nil {
			allErrors = multierror.Append(allErrors, oops.Wrapf(err, "describe table statistics for task: %s", taskId))
			continue
		}

		for _, table := range tables {
			tableTags := append(append([]string(nil), taskTags...),
				fmt.Sprintf("schema:%s", aws.StringValue(table.SchemaName)),
				fmt.Sprintf("table:%s", aws.StringValue(table.TableName)),
			)
			state := aws.StringValue(table.TableState)

			loading := float64(0)
			switch state {
			case "Full load", "Table completed":
				loading = 1
			}
			metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.task.table.loading", loading, tableTags))

			failed := float64(0)
			switch state {
			case "Table does not exist", "Table cancelled", "Table error":
				failed = 1
			}
			metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.task.table.failed", failed, tableTags))

			metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.task.table.ddls", float64(aws.Int64Value(table.Ddls)), tableTags))
			metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.task.table.deletes", float64(aws.Int64Value(table.Deletes)), tableTags))
			metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.task.table.inserts", float64(aws.Int64Value(table.Inserts)), tableTags))
			metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.task.table.updates", float64(aws.Int64Value(table.Updates)), tableTags))
			metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.task.table.full_load_rows", float64(aws.Int64Value(table.FullLoadRows)), tableTags))

			if table.LastUpdateTime != nil {
				metrics = append(metrics, datadoghttp.GaugeMetric(now, "dmsmetrics.task.table.last_update_age", time.Since(aws.TimeValue(table.LastUpdateTime)).Seconds(), tableTags))
			}
		}
	}

	return metrics, allErrors
}

func init() {
	fxregistry.MustRegisterDefaultConstructor(New)
}
