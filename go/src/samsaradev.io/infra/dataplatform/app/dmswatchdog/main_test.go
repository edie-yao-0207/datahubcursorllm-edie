package main

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/databasemigrationservice"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	mockcloudwatch "samsaradev.io/vendormocks/mock_cloudwatchiface"
	mockdms "samsaradev.io/vendormocks/mock_databasemigrationserviceiface"
)

func TestWatchdog_RunOnce_ResumesOnlyFailedOrStopped(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dms := mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	w := &Watchdog{
		region:      "us-west-2",
		dmsClient:   dms,
		limiter:     rate.NewLimiter(rate.Inf, 1),
		skipLatency: true, // Skip latency check for this test
	}

	tasks := []*databasemigrationservice.ReplicationTask{
		{ReplicationTaskArn: aws.String("arn:failed"), ReplicationTaskIdentifier: aws.String("task-failed"), Status: aws.String("failed"), MigrationType: aws.String("cdc")},
		{ReplicationTaskArn: aws.String("arn:stopped"), ReplicationTaskIdentifier: aws.String("task-stopped"), Status: aws.String("stopped"), MigrationType: aws.String("cdc")},
		{ReplicationTaskArn: aws.String("arn:running"), ReplicationTaskIdentifier: aws.String("task-running"), Status: aws.String("running"), MigrationType: aws.String("cdc")},
		{ReplicationTaskArn: aws.String(""), ReplicationTaskIdentifier: aws.String("task-no-arn"), Status: aws.String("failed"), MigrationType: aws.String("cdc")}, // missing arn should be skipped
	}

	dms.EXPECT().
		DescribeReplicationTasksPagesWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *databasemigrationservice.DescribeReplicationTasksInput, fn func(*databasemigrationservice.DescribeReplicationTasksOutput, bool) bool, _ ...request.Option) error {
			fn(&databasemigrationservice.DescribeReplicationTasksOutput{ReplicationTasks: tasks}, true)
			return nil
		}).
		Times(1)

	var started []string
	dms.EXPECT().
		StartReplicationTaskWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *databasemigrationservice.StartReplicationTaskInput, _ ...request.Option) (*databasemigrationservice.StartReplicationTaskOutput, error) {
			require.Equal(t, databasemigrationservice.StartReplicationTaskTypeValueResumeProcessing, aws.StringValue(in.StartReplicationTaskType))
			started = append(started, aws.StringValue(in.ReplicationTaskArn))
			return &databasemigrationservice.StartReplicationTaskOutput{}, nil
		}).
		Times(2)

	err := w.RunOnce(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"arn:failed", "arn:stopped"}, started)
}

func TestWatchdog_RunOnce_FailureReturnsError(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dms := mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	w := &Watchdog{
		region:      "us-west-2",
		dmsClient:   dms,
		limiter:     rate.NewLimiter(rate.Inf, 1),
		skipLatency: true, // Skip latency check for this test
	}

	tasks := []*databasemigrationservice.ReplicationTask{
		{ReplicationTaskArn: aws.String("arn:failed"), ReplicationTaskIdentifier: aws.String("task-failed"), Status: aws.String("failed"), MigrationType: aws.String("cdc")},
	}

	dms.EXPECT().
		DescribeReplicationTasksPagesWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *databasemigrationservice.DescribeReplicationTasksInput, fn func(*databasemigrationservice.DescribeReplicationTasksOutput, bool) bool, _ ...request.Option) error {
			fn(&databasemigrationservice.DescribeReplicationTasksOutput{ReplicationTasks: tasks}, true)
			return nil
		}).
		Times(1)

	dms.EXPECT().
		StartReplicationTaskWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, awserr.New("AccessDeniedException", "denied", nil)).
		Times(1)

	err := w.RunOnce(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tasksFailed=1")
}

func TestWatchdog_RunOnce_OnlyResumesCDCTasks(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dms := mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	w := &Watchdog{
		region:      "us-west-2",
		dmsClient:   dms,
		limiter:     rate.NewLimiter(rate.Inf, 1),
		skipLatency: true, // Skip latency check for this test
	}

	tasks := []*databasemigrationservice.ReplicationTask{
		// CDC task - should be resumed
		{ReplicationTaskArn: aws.String("arn:cdc-failed"), ReplicationTaskIdentifier: aws.String("cdc-task"), Status: aws.String("failed"), MigrationType: aws.String("cdc")},
		// Full-load task - should be skipped
		{ReplicationTaskArn: aws.String("arn:fullload-failed"), ReplicationTaskIdentifier: aws.String("fullload-task"), Status: aws.String("failed"), MigrationType: aws.String("full-load")},
		// Full-load-and-cdc task - should be skipped
		{ReplicationTaskArn: aws.String("arn:fullload-cdc-failed"), ReplicationTaskIdentifier: aws.String("fullload-cdc-task"), Status: aws.String("failed"), MigrationType: aws.String("full-load-and-cdc")},
		// CDC stopped task - should be resumed
		{ReplicationTaskArn: aws.String("arn:cdc-stopped"), ReplicationTaskIdentifier: aws.String("cdc-stopped-task"), Status: aws.String("stopped"), MigrationType: aws.String("cdc")},
	}

	dms.EXPECT().
		DescribeReplicationTasksPagesWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *databasemigrationservice.DescribeReplicationTasksInput, fn func(*databasemigrationservice.DescribeReplicationTasksOutput, bool) bool, _ ...request.Option) error {
			fn(&databasemigrationservice.DescribeReplicationTasksOutput{ReplicationTasks: tasks}, true)
			return nil
		}).
		Times(1)

	var started []string
	dms.EXPECT().
		StartReplicationTaskWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, in *databasemigrationservice.StartReplicationTaskInput, _ ...request.Option) (*databasemigrationservice.StartReplicationTaskOutput, error) {
			started = append(started, aws.StringValue(in.ReplicationTaskArn))
			return &databasemigrationservice.StartReplicationTaskOutput{}, nil
		}).
		Times(2) // Only CDC tasks should be resumed

	err := w.RunOnce(ctx)
	require.NoError(t, err)

	// Only the two CDC tasks should have been started
	assert.ElementsMatch(t, []string{"arn:cdc-failed", "arn:cdc-stopped"}, started)

	// Verify non-CDC tasks were NOT started
	assert.NotContains(t, started, "arn:fullload-failed")
	assert.NotContains(t, started, "arn:fullload-cdc-failed")
}

func TestWatchdog_LatencyMonitoring_OnlyChecksCDCTasks(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dms := mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	cw := mockcloudwatch.NewMockCloudWatchAPI(ctrl)
	w := &Watchdog{
		region:         "us-west-2",
		dmsClient:      dms,
		cloudwatchAPI:  cw,
		limiter:        rate.NewLimiter(rate.Inf, 1),
		skipResume:     true, // Skip resume for this test
		upsizeCooldown: make(map[string]time.Time),
	}

	tasks := []*databasemigrationservice.ReplicationTask{
		// CDC running task - should check latency
		{
			ReplicationTaskArn:        aws.String("arn:aws:dms:us-west-2:123:task:cdc-task"),
			ReplicationTaskIdentifier: aws.String("cdc-task"),
			Status:                    aws.String("running"),
			MigrationType:             aws.String("cdc"),
			ReplicationInstanceArn:    aws.String("arn:instance:1"),
		},
		// Full-load running task - should skip
		{
			ReplicationTaskArn:        aws.String("arn:aws:dms:us-west-2:123:task:fullload-task"),
			ReplicationTaskIdentifier: aws.String("fullload-task"),
			Status:                    aws.String("running"),
			MigrationType:             aws.String("full-load"),
			ReplicationInstanceArn:    aws.String("arn:instance:2"),
		},
		// Full-load-and-cdc running task - should skip
		{
			ReplicationTaskArn:        aws.String("arn:aws:dms:us-west-2:123:task:fullload-cdc-task"),
			ReplicationTaskIdentifier: aws.String("fullload-cdc-task"),
			Status:                    aws.String("running"),
			MigrationType:             aws.String("full-load-and-cdc"),
			ReplicationInstanceArn:    aws.String("arn:instance:3"),
		},
	}

	dms.EXPECT().
		DescribeReplicationTasksPagesWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *databasemigrationservice.DescribeReplicationTasksInput, fn func(*databasemigrationservice.DescribeReplicationTasksOutput, bool) bool, _ ...request.Option) error {
			fn(&databasemigrationservice.DescribeReplicationTasksOutput{ReplicationTasks: tasks}, true)
			return nil
		}).
		Times(1)

	dms.EXPECT().
		DescribeReplicationInstancesPagesWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *databasemigrationservice.DescribeReplicationInstancesInput, fn func(*databasemigrationservice.DescribeReplicationInstancesOutput, bool) bool, _ ...request.Option) error {
			fn(&databasemigrationservice.DescribeReplicationInstancesOutput{
				ReplicationInstances: []*databasemigrationservice.ReplicationInstance{
					{
						ReplicationInstanceArn:        aws.String("arn:instance:1"),
						ReplicationInstanceIdentifier: aws.String("instance-1"),
					},
					{
						ReplicationInstanceArn:        aws.String("arn:instance:2"),
						ReplicationInstanceIdentifier: aws.String("instance-2"),
					},
					{
						ReplicationInstanceArn:        aws.String("arn:instance:3"),
						ReplicationInstanceIdentifier: aws.String("instance-3"),
					},
				},
			}, true)
			return nil
		}).
		Times(1)

	// CloudWatch should only be called for the CDC task (twice: once for source, once for target)
	var queriedTasks []string
	cw.EXPECT().
		GetMetricStatisticsWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *cloudwatch.GetMetricStatisticsInput, _ ...request.Option) (*cloudwatch.GetMetricStatisticsOutput, error) {
			// Extract task ID from dimensions
			for _, dim := range input.Dimensions {
				if aws.StringValue(dim.Name) == "ReplicationTaskIdentifier" {
					queriedTasks = append(queriedTasks, aws.StringValue(dim.Value))
				}
			}
			// Return low latency (below threshold)
			return &cloudwatch.GetMetricStatisticsOutput{
				Datapoints: []*cloudwatch.Datapoint{
					{Maximum: aws.Float64(10)}, // 10 seconds, below 20 minute threshold
				},
			}, nil
		}).
		Times(2) // Only CDC task: 2 calls (CDCLatencySource + CDCLatencyTarget)

	err := w.RunOnce(ctx)
	require.NoError(t, err)

	// Only the CDC task should have been queried in CloudWatch
	assert.Contains(t, queriedTasks, "cdc-task")
	assert.NotContains(t, queriedTasks, "fullload-task")
	assert.NotContains(t, queriedTasks, "fullload-cdc-task")
}

func TestWatchdog_RunOnce_DoesNotShortCircuitLatencyOnResumeError(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dms := mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	cw := mockcloudwatch.NewMockCloudWatchAPI(ctrl)
	w := &Watchdog{
		region:         "us-west-2",
		dmsClient:      dms,
		cloudwatchAPI:  cw,
		limiter:        rate.NewLimiter(rate.Inf, 1),
		upsizeCooldown: make(map[string]time.Time),
	}

	tasks := []*databasemigrationservice.ReplicationTask{
		{
			ReplicationTaskArn:        aws.String("arn:aws:dms:us-west-2:123:task:cdc-failed"),
			ReplicationTaskIdentifier: aws.String("cdc-failed"),
			Status:                    aws.String("failed"),
			MigrationType:             aws.String("cdc"),
		},
		{
			ReplicationTaskArn:        aws.String("arn:aws:dms:us-west-2:123:task:cdc-running"),
			ReplicationTaskIdentifier: aws.String("cdc-running"),
			Status:                    aws.String("running"),
			MigrationType:             aws.String("cdc"),
			ReplicationInstanceArn:    aws.String("arn:instance:1"),
		},
	}

	dms.EXPECT().
		DescribeReplicationTasksPagesWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *databasemigrationservice.DescribeReplicationTasksInput, fn func(*databasemigrationservice.DescribeReplicationTasksOutput, bool) bool, _ ...request.Option) error {
			fn(&databasemigrationservice.DescribeReplicationTasksOutput{ReplicationTasks: tasks}, true)
			return nil
		}).
		Times(1)

	dms.EXPECT().
		StartReplicationTaskWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, awserr.New("AccessDeniedException", "denied", nil)).
		Times(1)

	dms.EXPECT().
		DescribeReplicationInstancesPagesWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *databasemigrationservice.DescribeReplicationInstancesInput, fn func(*databasemigrationservice.DescribeReplicationInstancesOutput, bool) bool, _ ...request.Option) error {
			fn(&databasemigrationservice.DescribeReplicationInstancesOutput{
				ReplicationInstances: []*databasemigrationservice.ReplicationInstance{
					{
						ReplicationInstanceArn:        aws.String("arn:instance:1"),
						ReplicationInstanceIdentifier: aws.String("instance-1"),
					},
				},
			}, true)
			return nil
		}).
		Times(1)

	var queriedTasks []string
	cw.EXPECT().
		GetMetricStatisticsWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, input *cloudwatch.GetMetricStatisticsInput, _ ...request.Option) (*cloudwatch.GetMetricStatisticsOutput, error) {
			for _, dim := range input.Dimensions {
				if aws.StringValue(dim.Name) == "ReplicationTaskIdentifier" {
					queriedTasks = append(queriedTasks, aws.StringValue(dim.Value))
				}
			}
			return &cloudwatch.GetMetricStatisticsOutput{
				Datapoints: []*cloudwatch.Datapoint{
					{Maximum: aws.Float64(10)},
				},
			}, nil
		}).
		Times(2)

	err := w.RunOnce(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tasksFailed=1")
	assert.Contains(t, queriedTasks, "cdc-running")
}

func TestWatchdog_RunOnce_DoesNotShortCircuitResumeOnLatencyError(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dms := mockdms.NewMockDatabaseMigrationServiceAPI(ctrl)
	cw := mockcloudwatch.NewMockCloudWatchAPI(ctrl)
	w := &Watchdog{
		region:         "us-west-2",
		dmsClient:      dms,
		cloudwatchAPI:  cw,
		limiter:        rate.NewLimiter(rate.Inf, 1),
		upsizeCooldown: make(map[string]time.Time),
	}

	tasks := []*databasemigrationservice.ReplicationTask{
		{
			ReplicationTaskArn:        aws.String("arn:aws:dms:us-west-2:123:task:cdc-failed"),
			ReplicationTaskIdentifier: aws.String("cdc-failed"),
			Status:                    aws.String("failed"),
			MigrationType:             aws.String("cdc"),
		},
		{
			ReplicationTaskArn:        aws.String("arn:aws:dms:us-west-2:123:task:cdc-running"),
			ReplicationTaskIdentifier: aws.String("cdc-running"),
			Status:                    aws.String("running"),
			MigrationType:             aws.String("cdc"),
			ReplicationInstanceArn:    aws.String("arn:instance:1"),
		},
	}

	dms.EXPECT().
		DescribeReplicationTasksPagesWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *databasemigrationservice.DescribeReplicationTasksInput, fn func(*databasemigrationservice.DescribeReplicationTasksOutput, bool) bool, _ ...request.Option) error {
			fn(&databasemigrationservice.DescribeReplicationTasksOutput{ReplicationTasks: tasks}, true)
			return nil
		}).
		Times(1)

	dms.EXPECT().
		StartReplicationTaskWithContext(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&databasemigrationservice.StartReplicationTaskOutput{}, nil).
		Times(1)

	dms.EXPECT().
		DescribeReplicationInstancesPagesWithContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(awserr.New("AccessDeniedException", "denied", nil)).
		Times(1)

	err := w.RunOnce(ctx)
	require.Error(t, err)
}
