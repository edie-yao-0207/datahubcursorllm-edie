package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice/databasemigrationserviceiface"
	"github.com/samsarahq/go/oops"
	"go.uber.org/fx"
	"golang.org/x/time/rate"

	"samsaradev.io/helpers/monitoring"
	"samsaradev.io/helpers/slog"
	"samsaradev.io/infra/config"
	"samsaradev.io/infra/samsaraaws/awshelpers/awssessions"
	"samsaradev.io/infra/serviceorchestration/leaderelection"
	"samsaradev.io/system"
)

const (
	latencyThresholdSeconds = 20 * 60 // 20 minutes
	upsizeCooldownPeriod    = 50 * 60 // 50 minutes - prevent rapid upsizing
	statusFailed            = "failed"
	statusStopped           = "stopped"
	statusRunning           = "running"
	statusReplicating       = "replicating"
	migrationTypeCDC        = "cdc"
)

var errNoDatapoints = errors.New("no CloudWatch datapoints")

var (
	runOnceFlag          = flag.Bool("run-once", false, "Run once and exit (for local testing)")
	dryRunFlag           = flag.Bool("dry-run", false, "Dry run mode - log actions but don't execute them")
	skipResumeFlag       = flag.Bool("skip-resume", false, "Skip resume task remediation (test latency monitoring only)")
	skipLatencyFlag      = flag.Bool("skip-latency", false, "Skip latency monitoring (test resume only)")
	awsRegionFlag        = flag.String("region", "", "AWS region override (defaults to AWS_REGION env var or session default)")
	latencyThresholdFlag = flag.Int("latency-threshold", 20, "Latency threshold in minutes for upsizing")
)

type Watchdog struct {
	region        string
	dmsClient     databasemigrationserviceiface.DatabaseMigrationServiceAPI
	cloudwatchAPI cloudwatchiface.CloudWatchAPI
	limiter       *rate.Limiter
	dryRun        bool
	skipResume    bool
	skipLatency   bool

	// Track recently upsized instances to prevent flapping
	upsizeCooldown map[string]time.Time
}

func (w *Watchdog) RunOnce(ctx context.Context) error {
	slog.Infow(ctx, "starting dms watchdog scan",
		"dryRun", w.dryRun,
		"skipResume", w.skipResume,
		"skipLatency", w.skipLatency,
	)

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
		return oops.Wrapf(err, "describe replication tasks")
	}

	// Resume failed or stopped CDC tasks
	var runErrors []error
	if !w.skipResume {
		if err := w.handleFailedOrStoppedStatus(ctx, tasks); err != nil {
			runErrors = append(runErrors, oops.Wrapf(err, "handle failed or stopped tasks"))
		}
	} else {
		slog.Infow(ctx, "skipped task resume scan (--skip-resume)")
	}

	// Check for high latency and upsize instances if needed
	if !w.skipLatency {
		if err := w.handleHighLatency(ctx, tasks); err != nil {
			runErrors = append(runErrors, oops.Wrapf(err, "handle high latency"))
		}
	} else {
		slog.Infow(ctx, "skipped latency monitoring (--skip-latency)")
	}

	if len(runErrors) > 0 {
		return errors.Join(runErrors...)
	}
	return nil
}

func (w *Watchdog) handleFailedOrStoppedStatus(ctx context.Context, tasks []*databasemigrationservice.ReplicationTask) error {
	slog.Infow(ctx, "starting dms task resume scan")

	scanned := len(tasks)
	actionable := 0
	resumed := 0
	failed := 0

	for _, t := range tasks {
		status := aws.StringValue(t.Status)
		if status != statusFailed && status != statusStopped {
			continue
		}
		taskArn := aws.StringValue(t.ReplicationTaskArn)
		taskIdentifier := aws.StringValue(t.ReplicationTaskIdentifier)
		migrationType := aws.StringValue(t.MigrationType)

		if taskArn == "" {
			continue
		}

		// Only act on CDC-only tasks (not full-load or full-load-and-cdc)
		if migrationType != migrationTypeCDC {
			continue
		}

		actionable++

		slog.Infow(ctx, "attempting to resume replication task",
			"replicationTaskArn", taskArn,
			"replicationTaskIdentifier", taskIdentifier,
			"migrationType", migrationType,
			"status", status,
			"dryRun", w.dryRun,
		)

		// Emit metrics for action attempts/outcomes so dashboards can show actions taken
		// by signal type and action type.
		monitoring.AggregatedDatadog.Incr(
			"dmswatchdog.action",
			"region:"+w.region,
			"signal:"+status,
			"action:resume",
			"outcome:attempt",
			"task_identifier:"+taskIdentifier,
			"migration_type:"+migrationType,
		)

		if w.dryRun {
			slog.Infow(ctx, "[DRY RUN] Would resume replication task",
				"replicationTaskArn", taskArn,
				"replicationTaskIdentifier", taskIdentifier,
				"migrationType", migrationType,
				"status", status,
			)
			resumed++
			continue
		}

		if err := w.limiter.Wait(ctx); err != nil {
			failed++
			monitoring.AggregatedDatadog.Incr(
				"dmswatchdog.action",
				"region:"+w.region,
				"signal:"+status,
				"action:resume",
				"outcome:error",
				"task_identifier:"+taskIdentifier,
				"migration_type:"+migrationType,
			)
			slog.Errorw(ctx, oops.Wrapf(err, "rate limiter wait for resume: arn=%s identifier=%s migrationType=%s status=%s", taskArn, taskIdentifier, migrationType, status), nil)
			continue
		}

		_, err := w.dmsClient.StartReplicationTaskWithContext(ctx, &databasemigrationservice.StartReplicationTaskInput{
			ReplicationTaskArn:       aws.String(taskArn),
			StartReplicationTaskType: aws.String(databasemigrationservice.StartReplicationTaskTypeValueResumeProcessing),
		})
		if err != nil {
			failed++
			monitoring.AggregatedDatadog.Incr(
				"dmswatchdog.action",
				"region:"+w.region,
				"signal:"+status,
				"action:resume",
				"outcome:error",
				"task_identifier:"+taskIdentifier,
				"migration_type:"+migrationType,
			)
			slog.Errorw(ctx, oops.Wrapf(err, "resume replication task: arn=%s identifier=%s migrationType=%s status=%s", taskArn, taskIdentifier, migrationType, status), nil)
			continue
		}
		monitoring.AggregatedDatadog.Incr(
			"dmswatchdog.action",
			"region:"+w.region,
			"signal:"+status,
			"action:resume",
			"outcome:success",
			"task_identifier:"+taskIdentifier,
			"migration_type:"+migrationType,
		)
		slog.Infow(ctx, "✅ requested DMS resume-processing",
			"replicationTaskArn", taskArn,
			"replicationTaskIdentifier", taskIdentifier,
			"migrationType", migrationType,
			"status", status,
		)
		resumed++
	}

	slog.Infow(ctx, "completed dms watchdog task resume scan",
		"tasksScanned", scanned,
		"tasksActionable", actionable,
		"tasksResumed", resumed,
		"tasksFailed", failed,
	)

	if failed > 0 {
		return oops.Errorf("dms watchdog scan had failures (tasksFailed=%d)", failed)
	}
	return nil
}

func (w *Watchdog) handleHighLatency(ctx context.Context, tasks []*databasemigrationservice.ReplicationTask) error {
	slog.Infow(ctx, "starting dms latency check")

	instanceIdentifiers, err := w.getInstanceIdentifiers(ctx)
	if err != nil {
		return oops.Wrapf(err, "get replication instance identifiers")
	}

	// Group tasks by replication instance to avoid upsizing the same instance multiple times
	instanceLatency := make(map[string]struct {
		maxLatency      int64
		taskArns        []string
		taskIdentifiers []string
	})

	checked := 0
	highLatency := 0
	highLatencyTaskIdentifiers := make([]string, 0)

	for _, t := range tasks {
		status := aws.StringValue(t.Status)
		// Only check running tasks for latency
		if status != statusRunning && status != statusReplicating {
			continue
		}

		taskArn := aws.StringValue(t.ReplicationTaskArn)
		taskIdentifier := aws.StringValue(t.ReplicationTaskIdentifier)
		migrationType := aws.StringValue(t.MigrationType)
		instanceArn := aws.StringValue(t.ReplicationInstanceArn)

		if instanceArn == "" || taskArn == "" {
			continue
		}
		instanceIdentifier := instanceIdentifiers[instanceArn]
		if instanceIdentifier == "" {
			slog.Infow(ctx, "skipping latency check; missing instance identifier",
				"replicationTaskArn", taskArn,
				"replicationInstanceArn", instanceArn,
			)
			continue
		}
		// Only monitor latency for CDC-only tasks (not full-load or full-load-and-cdc)
		if migrationType != migrationTypeCDC {
			continue
		}

		checked++

		// Get CDC latency for this task
		// Note: This queries CloudWatch metrics for CDCLatencySource and CDCLatencyTarget
		// which are the actual replication lag metrics for DMS tasks
		maxLatency, err := w.getTaskLatency(ctx, taskArn, instanceIdentifier)
		if err != nil {
			if oops.Is(err, errNoDatapoints) {
				slog.Infow(ctx, "skipping latency check; no CloudWatch datapoints",
					"replicationTaskArn", taskArn,
					"replicationTaskIdentifier", taskIdentifier,
					"replicationInstanceIdentifier", instanceIdentifier,
					"migrationType", migrationType,
				)
			} else {
				slog.Errorw(ctx, oops.Wrapf(err, "get task latency: taskArn=%s identifier=%s migrationType=%s", taskArn, taskIdentifier, migrationType), nil)
			}
			continue
		}

		threshold := int64(*latencyThresholdFlag * 60) // Convert minutes to seconds
		if maxLatency > threshold {
			highLatency++
			highLatencyTaskIdentifiers = append(highLatencyTaskIdentifiers, taskIdentifier)
			entry := instanceLatency[instanceArn]
			if maxLatency > entry.maxLatency {
				entry.maxLatency = maxLatency
			}
			entry.taskArns = append(entry.taskArns, taskArn)
			entry.taskIdentifiers = append(entry.taskIdentifiers, taskIdentifier)
			instanceLatency[instanceArn] = entry

			slog.Infow(ctx, "detected high latency",
				"replicationTaskArn", taskArn,
				"replicationTaskIdentifier", taskIdentifier,
				"migrationType", migrationType,
				"replicationInstanceArn", instanceArn,
				"latencySeconds", maxLatency,
				"thresholdSeconds", threshold,
			)
		}
	}

	// Upsize instances with high latency
	upsized := 0
	upsizeFailed := 0

	for instanceArn, entry := range instanceLatency {
		// Check cooldown
		lastUpsize, exists := w.upsizeCooldown[instanceArn]
		if exists && time.Since(lastUpsize).Seconds() < upsizeCooldownPeriod {
			slog.Infow(ctx, "skipping upsize due to cooldown",
				"replicationInstanceArn", instanceArn,
				"affectedTaskIdentifiers", entry.taskIdentifiers,
				"lastUpsizeTime", lastUpsize,
				"cooldownRemaining", upsizeCooldownPeriod-time.Since(lastUpsize).Seconds(),
			)
			continue
		}

		// Emit metrics for each affected task
		for _, taskIdentifier := range entry.taskIdentifiers {
			monitoring.AggregatedDatadog.Incr(
				"dmswatchdog.action",
				"region:"+w.region,
				"signal:high_latency",
				"action:upsize",
				"outcome:attempt",
				"task_identifier:"+taskIdentifier,
				"migration_type:cdc",
			)
		}

		if err := w.limiter.Wait(ctx); err != nil {
			upsizeFailed++
			for _, taskIdentifier := range entry.taskIdentifiers {
				monitoring.AggregatedDatadog.Incr(
					"dmswatchdog.action",
					"region:"+w.region,
					"signal:high_latency",
					"action:upsize",
					"outcome:error",
					"task_identifier:"+taskIdentifier,
					"migration_type:cdc",
				)
			}
			slog.Errorw(ctx, oops.Wrapf(err, "rate limiter wait for upsize: arn=%s taskIdentifiers=%v", instanceArn, entry.taskIdentifiers), nil)
			continue
		}

		if err := w.upsizeInstance(ctx, instanceArn, entry.maxLatency, entry.taskArns, entry.taskIdentifiers); err != nil {
			upsizeFailed++
			for _, taskIdentifier := range entry.taskIdentifiers {
				monitoring.AggregatedDatadog.Incr(
					"dmswatchdog.action",
					"region:"+w.region,
					"signal:high_latency",
					"action:upsize",
					"outcome:error",
					"task_identifier:"+taskIdentifier,
					"migration_type:cdc",
				)
			}
			slog.Errorw(ctx, oops.Wrapf(err, "upsize instance: arn=%s taskIdentifiers=%v", instanceArn, entry.taskIdentifiers), nil)
			continue
		}

		for _, taskIdentifier := range entry.taskIdentifiers {
			monitoring.AggregatedDatadog.Incr(
				"dmswatchdog.action",
				"region:"+w.region,
				"signal:high_latency",
				"action:upsize",
				"outcome:success",
				"task_identifier:"+taskIdentifier,
				"migration_type:cdc",
			)
		}

		// Record successful upsize
		w.upsizeCooldown[instanceArn] = time.Now()

		upsized++
	}

	slog.Infow(ctx, "completed dms latency check",
		"tasksChecked", checked,
		"highLatencyTasks", highLatency,
		"highLatencyTaskIdentifiers", highLatencyTaskIdentifiers,
		"instancesUpsized", upsized,
		"instancesUpsizeFailed", upsizeFailed,
	)

	return nil
}

func (w *Watchdog) upsizeInstance(ctx context.Context, instanceArn string, latencySeconds int64, taskArns []string, taskIdentifiers []string) error {
	// Describe the instance to get current class
	out, err := w.dmsClient.DescribeReplicationInstancesWithContext(ctx, &databasemigrationservice.DescribeReplicationInstancesInput{
		Filters: []*databasemigrationservice.Filter{
			{
				Name:   aws.String("replication-instance-arn"),
				Values: []*string{aws.String(instanceArn)},
			},
		},
	})
	if err != nil {
		return oops.Wrapf(err, "describe replication instance")
	}
	if len(out.ReplicationInstances) != 1 {
		return oops.Errorf("expected 1 replication instance, got %d", len(out.ReplicationInstances))
	}

	ri := out.ReplicationInstances[0]
	currentClass := normalizeDmsInstanceClass(aws.StringValue(ri.ReplicationInstanceClass))
	if currentClass == "" {
		return oops.Errorf("replication instance missing class")
	}

	// Calculate next class
	nextClass := nextDmsInstanceClass(currentClass)
	if nextClass == "" {
		return oops.Errorf("cannot determine next class for %q; may be at max size", currentClass)
	}

	if nextClass == currentClass {
		return oops.Errorf("target class equals current class (%s); nothing to do", currentClass)
	}

	slog.Infow(ctx, "upsizing DMS instance due to high latency",
		"replicationInstanceArn", instanceArn,
		"currentClass", currentClass,
		"targetClass", nextClass,
		"latencySeconds", latencySeconds,
		"affectedTasks", len(taskArns),
		"affectedTaskIdentifiers", taskIdentifiers,
		"migrationType", "cdc",
		"dryRun", w.dryRun,
	)

	if w.dryRun {
		slog.Infow(ctx, "[DRY RUN] Would upsize DMS instance",
			"replicationInstanceArn", instanceArn,
			"currentClass", currentClass,
			"targetClass", nextClass,
			"latencySeconds", latencySeconds,
			"affectedTaskIdentifiers", taskIdentifiers,
			"migrationType", "cdc",
		)
		return nil
	}

	// Perform the upsize
	_, err = w.dmsClient.ModifyReplicationInstanceWithContext(ctx, &databasemigrationservice.ModifyReplicationInstanceInput{
		ReplicationInstanceArn:   aws.String(instanceArn),
		ReplicationInstanceClass: aws.String(nextClass),
		ApplyImmediately:         aws.Bool(true),
	})
	if err != nil {
		return oops.Wrapf(err, "modify replication instance")
	}

	slog.Infow(ctx, "✅ requested DMS instance upsize",
		"replicationInstanceArn", instanceArn,
		"currentClass", currentClass,
		"targetClass", nextClass,
		"latencySeconds", latencySeconds,
		"affectedTaskIdentifiers", taskIdentifiers,
		"migrationType", "cdc",
	)

	return nil
}

func (w *Watchdog) getTaskLatency(ctx context.Context, taskArn, instanceIdentifier string) (int64, error) {
	if taskArn == "" {
		return 0, oops.Errorf("missing replication task arn")
	}
	if instanceIdentifier == "" {
		return 0, oops.Errorf("missing replication instance identifier")
	}

	taskID := extractTaskIDFromArn(taskArn)
	if taskID == "" {
		return 0, oops.Errorf("failed to extract replication task ID from arn: %s", taskArn)
	}

	// This sets the window from 1 hour ago till 2 minutes ago which is used by
	// the getMetricMaximum function to get the highest latency value in the last hour
	// and remediate it by upsizing the instance.
	now := time.Now().UTC()
	endTime := now.Add(-2 * time.Minute)     // allow for CloudWatch publishing delay
	startTime := endTime.Add(-1 * time.Hour) // widen window to capture recent datapoints

	// Query both CDCLatencySource and CDCLatencyTarget metrics
	sourceLatency, err := w.getMetricMaximum(ctx, "CDCLatencySource", taskID, instanceIdentifier, startTime, endTime)
	if err != nil {
		return 0, oops.Wrapf(err, "get CDCLatencySource metric")
	}

	targetLatency, err := w.getMetricMaximum(ctx, "CDCLatencyTarget", taskID, instanceIdentifier, startTime, endTime)
	if err != nil {
		return 0, oops.Wrapf(err, "get CDCLatencyTarget metric")
	}

	// Return the maximum of the two latencies (in seconds)
	maxLatency := sourceLatency
	if targetLatency > maxLatency {
		maxLatency = targetLatency
	}

	return maxLatency, nil
}

func (w *Watchdog) getMetricMaximum(ctx context.Context, metricName, taskID, instanceIdentifier string, startTime, endTime time.Time) (int64, error) {
	input := &cloudwatch.GetMetricStatisticsInput{
		Namespace:  aws.String("AWS/DMS"),
		MetricName: aws.String(metricName),
		Dimensions: []*cloudwatch.Dimension{
			{
				Name:  aws.String("ReplicationTaskIdentifier"),
				Value: aws.String(taskID),
			},
			{
				Name:  aws.String("ReplicationInstanceIdentifier"),
				Value: aws.String(instanceIdentifier),
			},
		},
		StartTime:  aws.Time(startTime),
		EndTime:    aws.Time(endTime),
		Period:     aws.Int64(300), // 5-minute periods
		Statistics: []*string{aws.String("Maximum")},
	}

	out, err := w.cloudwatchAPI.GetMetricStatisticsWithContext(ctx, input)
	if err != nil {
		return 0, oops.Wrapf(err, "get metric statistics: metric=%s taskID=%s instanceIdentifier=%s", metricName, taskID, instanceIdentifier)
	}

	if len(out.Datapoints) == 0 {
		return 0, oops.Wrapf(
			errNoDatapoints,
			"no datapoints: metric=%s taskID=%s instanceIdentifier=%s start=%s end=%s",
			metricName,
			taskID,
			instanceIdentifier,
			startTime.Format(time.RFC3339),
			endTime.Format(time.RFC3339),
		)
	}

	// Find the maximum value across all datapoints
	var maxValue float64
	for _, dp := range out.Datapoints {
		if dp.Maximum != nil && *dp.Maximum > maxValue {
			maxValue = *dp.Maximum
		}
	}

	// Convert from seconds (CloudWatch returns in seconds) to int64
	return int64(maxValue), nil
}

func extractTaskIDFromArn(arn string) string {
	// ARN format: arn:aws:dms:region:account:task:TASK_ID
	parts := strings.Split(arn, ":")
	if len(parts) < 6 {
		return ""
	}
	// The last part after "task:" is the task ID
	if parts[len(parts)-2] == "task" {
		return parts[len(parts)-1]
	}
	return ""
}

func (w *Watchdog) getInstanceIdentifiers(ctx context.Context) (map[string]string, error) {
	identifiers := make(map[string]string)
	input := &databasemigrationservice.DescribeReplicationInstancesInput{}

	err := w.dmsClient.DescribeReplicationInstancesPagesWithContext(ctx, input, func(out *databasemigrationservice.DescribeReplicationInstancesOutput, _ bool) bool {
		for _, instance := range out.ReplicationInstances {
			instanceArn := aws.StringValue(instance.ReplicationInstanceArn)
			instanceIdentifier := aws.StringValue(instance.ReplicationInstanceIdentifier)
			if instanceArn == "" || instanceIdentifier == "" {
				continue
			}
			identifiers[instanceArn] = instanceIdentifier
		}
		return true
	})
	if err != nil {
		return nil, oops.Wrapf(err, "describe replication instances")
	}

	return identifiers, nil
}

// Helper functions from dms_upsize.go
func nextDmsInstanceClass(current string) string {
	c := strings.TrimSpace(strings.ToLower(current))
	if c == "" {
		return ""
	}

	// Common ladders. If you need another family, manual intervention required.
	ladders := [][]string{
		{"dms.t3.micro", "dms.t3.small", "dms.t3.medium", "dms.t3.large", "dms.t3.xlarge", "dms.t3.2xlarge"},
		{"dms.r5.large", "dms.r5.xlarge", "dms.r5.2xlarge", "dms.r5.4xlarge", "dms.r5.8xlarge", "dms.r5.12xlarge", "dms.r5.16xlarge", "dms.r5.24xlarge"},
		{"dms.r6i.large", "dms.r6i.xlarge", "dms.r6i.2xlarge", "dms.r6i.4xlarge", "dms.r6i.8xlarge", "dms.r6i.12xlarge", "dms.r6i.16xlarge", "dms.r6i.24xlarge", "dms.r6i.32xlarge"},
	}

	for _, ladder := range ladders {
		for i := 0; i < len(ladder); i++ {
			if ladder[i] != c {
				continue
			}
			if i == len(ladder)-1 {
				return ""
			}
			return ladder[i+1]
		}
	}

	return ""
}

func normalizeDmsInstanceClass(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func main() {
	flag.Parse()

	// Run-once mode for local testing (bypasses fx and leader election)
	if *runOnceFlag {
		runOnceMode()
		return
	}

	// Normal production mode with fx and leader election
	system.NewFx(&config.ConfigParams{},
		fx.Provide(
			func() *Watchdog {
				sess := awssessions.NewInstrumentedAWSSession()
				region := aws.StringValue(sess.Config.Region)
				if *awsRegionFlag != "" {
					region = *awsRegionFlag
				}
				dmsClient := databasemigrationservice.New(sess)
				cloudwatchClient := cloudwatch.New(sess)

				// DMS API has shared rate limits; keep this conservative.
				return &Watchdog{
					region:         region,
					dmsClient:      dmsClient,
					cloudwatchAPI:  cloudwatchClient,
					limiter:        rate.NewLimiter(rate.Limit(5), 1),
					dryRun:         *dryRunFlag,
					skipResume:     *skipResumeFlag,
					skipLatency:    *skipLatencyFlag,
					upsizeCooldown: make(map[string]time.Time),
				}
			},
		),
		fx.Invoke(func(lc fx.Lifecycle, w *Watchdog, leaderElectionHelper leaderelection.WaitForLeaderLifecycleHookHelper) {
			// Run periodically, but only on the leader to avoid duplicate remediation.
			// Checks both failed/stopped tasks (resume) and high latency tasks (upsize).
			lc.Append(leaderElectionHelper.RunAfterLeaderElectionLifecycleHook(func(ctx context.Context) {
				ticker := time.NewTicker(30 * time.Minute)
				defer ticker.Stop()

				for {
					// Keep run timeout shorter than tick interval so runs don't overlap.
					runCtx, cancel := context.WithTimeout(ctx, 25*time.Minute)
					if err := w.RunOnce(runCtx); err != nil && oops.Cause(err) != context.Canceled {
						slog.Errorw(ctx, oops.Wrapf(err, "dms watchdog run failed"), nil)
					}
					cancel()

					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
					}
				}
			}))
		}),
	).Run()
}

func runOnceMode() {
	ctx := context.Background()

	// Set up AWS session
	var sess *aws.Config
	if *awsRegionFlag != "" {
		sess = &aws.Config{Region: aws.String(*awsRegionFlag)}
	}

	awsSession := awssessions.NewInstrumentedAWSSessionWithConfigs(sess)
	region := aws.StringValue(awsSession.Config.Region)
	dmsClient := databasemigrationservice.New(awsSession)
	cloudwatchClient := cloudwatch.New(awsSession)

	fmt.Printf("🚀 Running DMS Watchdog in run-once mode\n")
	fmt.Printf("   Region: %s\n", region)
	fmt.Printf("   Dry Run: %v\n", *dryRunFlag)
	fmt.Printf("   Skip Resume: %v\n", *skipResumeFlag)
	fmt.Printf("   Skip Latency: %v\n", *skipLatencyFlag)
	fmt.Printf("   Latency Threshold: %d minutes\n\n", *latencyThresholdFlag)

	w := &Watchdog{
		region:         region,
		dmsClient:      dmsClient,
		cloudwatchAPI:  cloudwatchClient,
		limiter:        rate.NewLimiter(rate.Limit(5), 1),
		dryRun:         *dryRunFlag,
		skipResume:     *skipResumeFlag,
		skipLatency:    *skipLatencyFlag,
		upsizeCooldown: make(map[string]time.Time),
	}

	if err := w.RunOnce(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n✅ Completed successfully\n")
}
