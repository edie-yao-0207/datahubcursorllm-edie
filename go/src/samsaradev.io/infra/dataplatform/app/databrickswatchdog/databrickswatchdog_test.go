package main

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/helpers/testinghelpers"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/databricks/mock_databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/testloader"
)

func TestGetCustomTagValue(t *testing.T) {
	testCases := []struct {
		description string
		tags        map[string]string
		tagName     string
		expected    string
	}{
		{
			description: "should return dynamodb_deltalake_ingestion_merge for dynamodb merge jobs",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
				"samsara:pooled-job:dataplatform-job-type": "dynamodb_deltalake_ingestion_merge",
			},
			tagName:  dataplatformconsts.DATAPLAT_JOBTYPE_TAG,
			expected: "dynamodb_deltalake_ingestion_merge",
		},
		{
			description: "should return empty string if `samsara:pooled-job:dataplatform-job-type` doesn't exist",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
			},
			tagName:  dataplatformconsts.DATAPLAT_JOBTYPE_TAG,
			expected: "",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			actual := getCustomTagValue(tc.tags, tc.tagName)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestContainsDataPlatformMergeJobTag(t *testing.T) {
	testCases := []struct {
		description string
		tags        map[string]string
		expected    bool
	}{
		{
			description: "should return false for non-existent tag",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
			},
			expected: false,
		},
		{
			description: "should return true for kinesisstats_deltalake_ingestion_merge",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
				"samsara:pooled-job:dataplatform-job-type": string(dataplatformconsts.KinesisStatsDeltaLakeIngestionMerge),
			},
			expected: true,
		},
		{
			description: "should return true for rds_deltalake_ingestion_merge",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
				"samsara:pooled-job:dataplatform-job-type": string(dataplatformconsts.RdsDeltaLakeIngestionMerge),
			},
			expected: true,
		},
		{
			description: "should return true for dynamodb_deltalake_ingestion_merge",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
				"samsara:pooled-job:dataplatform-job-type": string(dataplatformconsts.DynamoDbDeltaLakeIngestionMerge),
			},
			expected: true,
		},
		{
			description: "should return true for kinesisstats_bigstats_deltalake_ingestion_merge",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
				"samsara:pooled-job:dataplatform-job-type": string(dataplatformconsts.KinesisStatsBigStatsDeltaLakeIngestionMerge),
			},
			expected: true,
		},
		{
			description: "should return false for data_pipelines_sqlite_node",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
				"samsara:pooled-job:dataplatform-job-type": string(dataplatformconsts.DataPipelinesSqliteNode),
			},
			expected: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			actual := containsDataPlatformMergeJobTag(tc.tags)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestUpsizeAutoscaleConfiguration(t *testing.T) {
	testCases := []struct {
		description     string
		autoscaleConfig databricks.ClusterAutoScale
		expected        databricks.ClusterAutoScale
	}{
		{
			description: "should quadruple the current autoscale max workers configuration",
			autoscaleConfig: databricks.ClusterAutoScale{
				MinWorkers: 2,
				MaxWorkers: 10,
			},
			expected: databricks.ClusterAutoScale{
				MinWorkers: 2,
				MaxWorkers: 40,
			},
		},
		{
			description: "should not exceed max of 128 workers",
			autoscaleConfig: databricks.ClusterAutoScale{
				MinWorkers: 100,
				MaxWorkers: 128,
			},
			expected: databricks.ClusterAutoScale{
				MinWorkers: 100,
				MaxWorkers: 128,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			actual := upsizeAutoscaleConfiguration(tc.autoscaleConfig)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestIsClusterSizeUnchanged(t *testing.T) {
	testCases := []struct {
		description                       string
		jobSettingCluster, currentCluster *databricks.ClusterAutoScale
		expected                          bool
	}{
		{
			description: "should return true if job setting cluster autoscale configuration matches the current cluster autoscale configuration",
			jobSettingCluster: &databricks.ClusterAutoScale{
				MinWorkers: 1,
				MaxWorkers: 16,
			},
			currentCluster: &databricks.ClusterAutoScale{
				MinWorkers: 1,
				MaxWorkers: 16,
			},
			expected: true,
		},
		{
			description: "should return false if job setting cluster's min workers does not match the current cluster's min workers",
			jobSettingCluster: &databricks.ClusterAutoScale{
				MinWorkers: 1,
				MaxWorkers: 16,
			},
			currentCluster: &databricks.ClusterAutoScale{
				MinWorkers: 2,
				MaxWorkers: 16,
			},
			expected: false,
		},
		{
			description: "should return false if job setting cluster's max workers does not match the current cluster's max workers",
			jobSettingCluster: &databricks.ClusterAutoScale{
				MinWorkers: 1,
				MaxWorkers: 2,
			},
			currentCluster: &databricks.ClusterAutoScale{
				MinWorkers: 1,
				MaxWorkers: 16,
			},
			expected: false,
		},
		{
			description: "should return false if job setting cluster autoscale configuration does not match the current cluster autoscale configuration",
			jobSettingCluster: &databricks.ClusterAutoScale{
				MinWorkers: 2,
				MaxWorkers: 5,
			},
			currentCluster: &databricks.ClusterAutoScale{
				MinWorkers: 1,
				MaxWorkers: 16,
			},
			expected: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			actual := isClusterSizeUnchanged(tc.jobSettingCluster, tc.currentCluster)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestGetUpsizeClusterDurationThreshold(t *testing.T) {
	testCases := []struct {
		description string
		tags        map[string]string
		expected    int
	}{
		{
			description: "should return 1hr for 0 slo-target hours",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
				"samsara:pooled-job:dataplatform-job-type": string(dataplatformconsts.DataPipelinesSqliteNode),
				"samsara:pooled-job:slo-target":            "0",
			},
			expected: 1 * samtime.MillisecondsInHour,
		},
		{
			description: "should return 1hr for missing slo-target hours tag",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
				"samsara:pooled-job:dataplatform-job-type": string(dataplatformconsts.DataPipelinesSqliteNode),
			},
			expected: 1 * samtime.MillisecondsInHour,
		},
		{
			description: "should return 1hr for 9 slo-target hours",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
				"samsara:pooled-job:dataplatform-job-type": string(dataplatformconsts.DataPipelinesSqliteNode),
				"samsara:pooled-job:slo-target":            "9",
			},
			expected: 1 * samtime.MillisecondsInHour,
		},
		{
			description: "should return 1hr for 10 slo-target hours",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
				"samsara:pooled-job:dataplatform-job-type": string(dataplatformconsts.DataPipelinesSqliteNode),
				"samsara:pooled-job:slo-target":            "10",
			},
			expected: 1 * samtime.MillisecondsInHour,
		},
		{
			description: "should return 2hrs for 19 slo-target hours",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
				"samsara:pooled-job:dataplatform-job-type": string(dataplatformconsts.DataPipelinesSqliteNode),
				"samsara:pooled-job:slo-target":            "19",
			},
			expected: 1 * samtime.MillisecondsInHour,
		},
		{
			description: "should return 2hrs for 20 slo-target hours",
			tags: map[string]string{
				"samsara:team":  "dataanalytics",
				"somerandomTag": "shouldNotShowUp",
				"samsara:pooled-job:dataplatform-job-type": string(dataplatformconsts.DataPipelinesSqliteNode),
				"samsara:pooled-job:slo-target":            "20",
			},
			expected: 2 * samtime.MillisecondsInHour,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			actual, err := getUpsizeClusterDurationThreshold("test-job-name", tc.tags)
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestGetDataPlatformMergeJobs(t *testing.T) {
	dbxClient := mock_databricks.NewMockAPI(gomock.NewController(t))
	var env struct {
		Clock *clock.Mock
	}
	testloader.MustStart(t, &env)
	now := testinghelpers.Nov3_12AM_2022PST
	env.Clock.Set(samtime.MsToTime(now))
	worker := Worker{
		databricksClient: dbxClient,
		clock:            env.Clock,
	}
	mergeJobs := []databricks.Job{
		{
			JobId: 100,
			JobSettings: databricks.JobSettings{
				Name: "job1",
				JobClusters: []*databricks.JobCluster{{NewCluster: &databricks.ClusterSpec{
					AwsAttributes: &databricks.ClusterAwsAttributes{
						ZoneId: "us-west-2a",
					},
					SparkVersion: "7.3.x-scala2.12",
					CustomTags: map[string]string{
						"samsara:pooled-job:pool-test":             "job1",
						"samsara:job:job-test":                     "job1",
						"samsara:pooled-job:dataplatform-job-type": "kinesisstats_deltalake_ingestion_merge",
						"samsara:pooled-job:is-production-job":     "true",
						"samsara:job:team":                         "owning_team",
						"samsara:team":                             "ignored_team",
						"ignored-tag":                              "shouldnt see this",
						"samsara:job:slo-target":                   "7",
						"samsara:job:low-urgency-threshold":        "7",
						"samsara:job:business-hours-threshold":     "24",
						"samsara:job:high-urgency-threshold":       "72",
						"samsara:job:database":                     "test_db",
						"samsara:job:table":                        "test_table",
					},
					AutoScale: &databricks.ClusterAutoScale{
						MinWorkers: 1,
						MaxWorkers: 2,
					},
				}}},
			},
		},
	}
	dbxClient.EXPECT().ListJobs(gomock.Any(), &databricks.ListJobsInput{ExpandTasks: true}).Return(&databricks.ListJobsOutput{
		Jobs: mergeJobs,
	}, nil).Times(1)
	actual, err := worker.getDataPlatformMergeJobs(context.Background())
	require.NoError(t, err)
	assert.Equal(t, mergeJobs, actual)
}

func TestUpsizeLongRunningMergeJobClusters(t *testing.T) {
	dbxClient := mock_databricks.NewMockAPI(gomock.NewController(t))
	var env struct {
		Clock *clock.Mock
	}
	testloader.MustStart(t, &env)
	now := testinghelpers.Nov3_12AM_2022PST
	env.Clock.Set(samtime.MsToTime(now))
	worker := Worker{
		databricksClient: dbxClient,
		clock:            env.Clock,
	}

	job1clusterspec := databricks.RunClusterSpec{
		NewCluster: &databricks.ClusterSpec{
			AwsAttributes: &databricks.ClusterAwsAttributes{
				ZoneId: "us-west-2a",
			},
			SparkVersion: "7.3.x-scala2.12",
			CustomTags: map[string]string{
				"samsara:pooled-job:pool-test":             "job1",
				"samsara:job:job-test":                     "job1",
				"samsara:pooled-job:dataplatform-job-type": "kinesisstats_deltalake_ingestion_merge",
				"samsara:pooled-job:is-production-job":     "true",
				"samsara:job:team":                         "owning_team",
				"samsara:team":                             "ignored_team",
				"ignored-tag":                              "shouldnt see this",
				"samsara:job:slo-target":                   "7",
				"samsara:job:low-urgency-threshold":        "7",
				"samsara:job:business-hours-threshold":     "24",
				"samsara:job:high-urgency-threshold":       "72",
				"samsara:job:database":                     "test_db",
				"samsara:job:table":                        "test_table",
			},
			AutoScale: &databricks.ClusterAutoScale{
				MinWorkers: 1,
				MaxWorkers: 2,
			},
		},
	}

	success := databricks.RunResultStateSuccess
	dbxClient.EXPECT().ListRuns(gomock.Any(), &databricks.ListRunsInput{
		JobId:       100,
		ActiveOnly:  true,
		ExpandTasks: true,
	}).Return(&databricks.ListRunsOutput{
		Runs: []*databricks.Run{
			{
				JobId: 100,
				RunId: 1,
				State: databricks.RunState{
					LifeCycleState: databricks.RunLifeCycleStateRunning,
					ResultState:    &success,
				},
				StartTime:         now - 2*samtime.MillisecondsInHour,
				EndTime:           now,
				SetupDuration:     5,
				ExecutionDuration: 90,
				CleanupDuration:   5,
				RunDuration:       int(samtime.MillisecondsInHour),
				RunType:           databricks.RunTypeJobRun,
				ClusterInstance: &databricks.ClusterInstance{
					ClusterId: "100",
				},
				JobClusters: []*databricks.JobCluster{
					{
						JobClusterKey: "default",
						NewCluster:    job1clusterspec.NewCluster,
					},
				},
				Tasks: []*databricks.RunTask{
					{
						RunId:         1,
						TaskKey:       "task-1",
						AttemptNumber: 0,
						JobClusterKey: "default",
						State: databricks.RunState{
							LifeCycleState: databricks.RunLifeCycleStateRunning,
							ResultState:    &success,
						},
						StartTime:         now - 2*samtime.MillisecondsInHour,
						EndTime:           now,
						SetupDuration:     5,
						ExecutionDuration: 90,
						CleanupDuration:   5,
						ClusterInstance: &databricks.ClusterInstance{
							ClusterId: "100",
						},
					},
				},
			},
		},
	}, nil).Times(1)

	dbxClient.EXPECT().GetCluster(gomock.Any(), &databricks.GetClusterInput{
		ClusterId: "100",
	}).Return(&databricks.GetClusterOutput{
		ClusterSpec: databricks.ClusterSpec{
			AutoScale: &databricks.ClusterAutoScale{
				MinWorkers: 1,
				MaxWorkers: 2,
			},
		},
	}, nil).Times(1)
	dbxClient.EXPECT().ResizeCluster(gomock.Any(), &databricks.ResizeClusterInput{
		Autoscale: databricks.ClusterAutoScale{
			MinWorkers: 1,
			MaxWorkers: 8,
		},
		ClusterId: "100",
	}).Return(&databricks.ResizeClusterOutput{
		ErrorCode: "",
		Message:   "",
	}, nil).Times(1)
	err := worker.upsizeLongRunningMergeJobClusters(context.Background(), []databricks.Job{
		{
			JobId: 100,
			JobSettings: databricks.JobSettings{
				Name:        "job1",
				JobClusters: []*databricks.JobCluster{{NewCluster: job1clusterspec.NewCluster}},
			},
		},
	})
	require.NoError(t, err)
}

func TestIsErrorRetriable(t *testing.T) {
	testCases := []struct {
		description string
		errMessage  string
		expected    bool
	}{
		{
			description: "should return true for aws insufficent instance capacity error message",
			errMessage:  `Task regulation_mode_driver_time_ranges-v0 failed with message: Cluster 1201-152702-6zjz069j was terminated while waiting on it to be ready: Cluster 1201-152702-6zjz069j is in unexpected state Terminated: AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE(CLIENT_ERROR): aws_api_error_code:InsufficientInstanceCapacity,aws_error_message:There is no Spot capacity available that matches your request.. This caused all downstream tasks to get skipped.`,
			expected:    true,
		},
		{
			description: "should return false for other error messages",
			// Actual error message for task run id: 954820185032095
			errMessage: "Workload failed, see run output for details",
			expected:   false,
		},
		{
			description: "should return false for empty string",
			errMessage:  "",
			expected:    false,
		},
		{
			description: "should return true for partition is no longer alive error message",
			errMessage:  "org.apache.spark.SparkException: Job aborted due to stage failure: Task 8 in stage 608.1 failed 4 times, most recent failure: Lost task 8.3 in stage 608.1 (TID 2740355) (10.48.55.206 executor 403): org.apache.spark.SparkException:  Checkpoint block rdd_1740_77 not found! Either the executor that originally checkpointed this partition is no longer alive, or the original RDD is unpersisted. If this problem persists, you may consider using `rdd.checkpoint()` instead, which is slower than local checkpointing but more fault-tolerant.",
			expected:    true,
		},
		{
			description: "should return true for sqlite3.IntegrityError",
			errMessage:  "org.apache.spark.SparkException: Job aborted due to stage failure: Task 2601 in stage 583.0 failed 4 times, most recent failure: Lost task 2601.3 in stage 583.0 (TID 40393) (10.48.71.186 executor 53): org.apache.spark.api.python.PythonException: 'sqlite3.IntegrityError: UNIQUE constraint failed: speeding_report_v3.month, speeding_report_v3.org_id, speeding_report_v3.duration_ms, speeding_report_v3.interval_start, speeding_report_v3.unit, speeding_report_v3.object_type, speeding_report_v3.object_id'. Full traceback below:",
			expected:    true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			actual := isErrorRetriable(tc.errMessage)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestIsJobEligibleForRetry(t *testing.T) {
	awsInsufficientInstanceCapacityFailureMsg := `Task regulation_mode_driver_time_ranges-v0 failed with message: Cluster 1201-152702-6zjz069j was terminated while waiting on it to be ready: Cluster 1201-152702-6zjz069j is in unexpected state Terminated: AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE(CLIENT_ERROR): aws_api_error_code:InsufficientInstanceCapacity,aws_error_message:There is no Spot capacity available that matches your request.. This caused all downstream tasks to get skipped.`
	partitionNoLongerAliveNonSanitizedFailureMsg := "Either the executor\nthat originally checkpointed this partition is no longer alive, or the original RDD is\nunpersisted. If this problem persists, you may consider using `rdd.checkpoint()`\ninstead, which is slower than local checkpointing but more fault-tolerant.\n"
	partitionNoLongerAliveSanitizedFailureMsg := "Either the executor that originally checkpointed this partition is no longer alive, or the original RDD is unpersisted. If this problem persists, you may consider using `rdd.checkpoint()` instead, which is slower than local checkpointing but more fault-tolerant."
	randomFailureMsg := `Some random error`
	failed := databricks.RunResultStateFailed
	succeeded := databricks.RunResultStateSuccess
	testCases := []struct {
		description  string
		runs         []*databricks.Run
		errorMessage string
		isProdTable  bool
		expected     bool
	}{
		{
			description: "should not retry for prod job if recent run succeeded",
			runs: []*databricks.Run{
				{
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &succeeded,
						StateMessage:   "",
					},
					Tasks: []*databricks.RunTask{
						{
							RunId: 1,
						},
					},
				},
			},
			errorMessage: "",
			isProdTable:  true,
			expected:     false,
		},
		{
			description: "should retry for prod job if recent run failed due to AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE",
			runs: []*databricks.Run{
				{
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateInternalError,
						ResultState:    &failed,
						StateMessage:   awsInsufficientInstanceCapacityFailureMsg,
					},
					Tasks: []*databricks.RunTask{
						{
							RunId: 1,
						},
					},
				},
			},
			errorMessage: awsInsufficientInstanceCapacityFailureMsg,
			isProdTable:  true,
			expected:     true,
		},
		{
			description: "should not retry for prod job if recent run failed due to errors we do not support retrying for",
			runs: []*databricks.Run{
				{
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateInternalError,
						ResultState:    &failed,
						StateMessage:   randomFailureMsg,
					},
					Tasks: []*databricks.RunTask{
						{
							RunId: 1,
						},
					},
				},
			},
			errorMessage: randomFailureMsg,
			isProdTable:  true,
			expected:     false,
		},
		{
			description: "should not retry for nonprod job if recent run succeeded",
			runs: []*databricks.Run{
				{
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &succeeded,
						StateMessage:   "",
					},
					Tasks: []*databricks.RunTask{
						{
							RunId: 1,
						},
					},
				},
			},
			errorMessage: "",
			isProdTable:  false,
			expected:     false,
		},
		{
			description: "should retry for nonprod job if recent run failed due to AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE",
			runs: []*databricks.Run{
				{
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateInternalError,
						ResultState:    &failed,
						StateMessage:   awsInsufficientInstanceCapacityFailureMsg,
					},
					Tasks: []*databricks.RunTask{
						{
							RunId: 1,
						},
					},
				},
			},
			errorMessage: awsInsufficientInstanceCapacityFailureMsg,
			isProdTable:  false,
			expected:     true,
		},
		{
			description: "should retry for nonprod job if recent run failed due to AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE",
			runs: []*databricks.Run{
				{
					RunId: 1,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateInternalError,
						ResultState:    &failed,
						StateMessage:   awsInsufficientInstanceCapacityFailureMsg,
					},
					Tasks: []*databricks.RunTask{
						{
							RunId: 1,
						},
					},
				},
			},
			errorMessage: awsInsufficientInstanceCapacityFailureMsg,
			isProdTable:  false,
			expected:     true,
		},
		{
			description: "should not retry for nonprod job if recent run failed due to errors we do not support retrying for",
			runs: []*databricks.Run{
				{
					RunId: 1,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateInternalError,
						ResultState:    &failed,
						StateMessage:   randomFailureMsg,
					},
					Tasks: []*databricks.RunTask{
						{
							RunId: 1,
						},
					},
				},
			},
			errorMessage: randomFailureMsg,
			isProdTable:  false,
			expected:     false,
		},
		{
			description: "should retry job if recent run failed due to partition no longer alive (with newline characters)",
			runs: []*databricks.Run{
				{
					RunId: 1,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateInternalError,
						ResultState:    &failed,
						StateMessage:   partitionNoLongerAliveNonSanitizedFailureMsg,
					},
					Tasks: []*databricks.RunTask{
						{
							RunId: 1,
						},
					},
				},
			},
			errorMessage: partitionNoLongerAliveNonSanitizedFailureMsg,
			isProdTable:  false,
			expected:     true,
		},
		{
			description: "should retry job if recent run failed due to partition no longer alive",
			runs: []*databricks.Run{
				{
					RunId: 1,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateInternalError,
						ResultState:    &failed,
						StateMessage:   partitionNoLongerAliveSanitizedFailureMsg,
					},
					Tasks: []*databricks.RunTask{
						{
							RunId: 1,
						},
					},
				},
			},
			errorMessage: partitionNoLongerAliveSanitizedFailureMsg,
			isProdTable:  false,
			expected:     true,
		},
		{
			description: "should not retry job if recent run result state is nil",
			runs: []*databricks.Run{
				{
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateInternalError,
						StateMessage:   randomFailureMsg,
						ResultState:    nil,
					},
					Tasks: []*databricks.RunTask{
						{
							RunId: 1,
						},
					},
				},
			},
			errorMessage: "",
			isProdTable:  false,
			expected:     false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			dbxClient := mock_databricks.NewMockAPI(gomock.NewController(t))
			var env struct {
				Clock *clock.Mock
			}
			testloader.MustStart(t, &env)
			worker := Worker{
				databricksClient: dbxClient,
				clock:            env.Clock,
			}
			if tc.runs[0].State.ResultState != nil && databricks.RunResultState(*tc.runs[0].State.ResultState) == databricks.RunResultStateFailed {
				dbxClient.EXPECT().GetRunOutput(gomock.Any(), &databricks.GetRunOutputInput{RunId: tc.runs[0].Tasks[0].RunId}).Return(&databricks.GetRunOutputOutput{
					ErrorTrace: tc.errorMessage,
				}, nil).AnyTimes()
			}
			actual, err := worker.isJobEligibleForRetry(context.Background(), tc.runs[0])
			require.NoError(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestPrioritizeProductionJobs(t *testing.T) {
	prodTable := func(jobId int64) JobRunsMetadata {
		return JobRunsMetadata{
			Job: databricks.Job{
				JobId: jobId,
			},
			isProdTable: true,
		}
	}
	nonProdTable := func(jobId int64) JobRunsMetadata {
		return JobRunsMetadata{
			Job: databricks.Job{
				JobId: jobId,
			},
			isProdTable: false,
		}
	}
	testCases := []struct {
		description string
		mergeJobs   []JobRunsMetadata
		expected    []JobRunsMetadata
	}{
		{
			description: "should prioritize production tables",
			mergeJobs: []JobRunsMetadata{
				nonProdTable(1),
				prodTable(2),
				nonProdTable(3),
				prodTable(4),
				nonProdTable(5),
			},
			expected: []JobRunsMetadata{
				prodTable(2),
				prodTable(4),
				nonProdTable(1),
				nonProdTable(3),
				nonProdTable(5),
			},
		},
		{
			description: "should return all production tables in same order it was received",
			mergeJobs: []JobRunsMetadata{
				prodTable(1),
				prodTable(2),
			},
			expected: []JobRunsMetadata{
				prodTable(1),
				prodTable(2),
			},
		},
		{
			description: "should return all nonproduction tables in same order it was received",
			mergeJobs: []JobRunsMetadata{
				nonProdTable(1),
				nonProdTable(2),
			},
			expected: []JobRunsMetadata{
				nonProdTable(1),
				nonProdTable(2),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			actual := prioritizeProductionJobs(tc.mergeJobs)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestRetryFailedMergeJobs(t *testing.T) {
	dbxClient := mock_databricks.NewMockAPI(gomock.NewController(t))
	var env struct {
		Clock *clock.Mock
	}
	testloader.MustStart(t, &env)
	now := testinghelpers.Nov3_12AM_2022PST
	env.Clock.Set(samtime.MsToTime(now))
	worker := Worker{
		databricksClient: dbxClient,
		clock:            env.Clock,
	}

	job1clusterspec := databricks.RunClusterSpec{
		NewCluster: &databricks.ClusterSpec{
			AwsAttributes: &databricks.ClusterAwsAttributes{
				ZoneId: "us-west-2a",
			},
			SparkVersion: "7.3.x-scala2.12",
			CustomTags: map[string]string{
				"samsara:pooled-job:pool-test":             "job1",
				"samsara:job:job-test":                     "job1",
				"samsara:pooled-job:dataplatform-job-type": "kinesisstats_deltalake_ingestion_merge",
				"samsara:pooled-job:is-production-job":     "true",
				"samsara:job:team":                         "owning_team",
				"samsara:team":                             "ignored_team",
				"ignored-tag":                              "shouldnt see this",
				"samsara:job:slo-target":                   "7",
				"samsara:job:low-urgency-threshold":        "7",
				"samsara:job:business-hours-threshold":     "24",
				"samsara:job:high-urgency-threshold":       "72",
				"samsara:job:database":                     "test_db",
				"samsara:job:table":                        "test_table",
			},
			AutoScale: &databricks.ClusterAutoScale{
				MinWorkers: 1,
				MaxWorkers: 2,
			},
		},
	}

	failed := databricks.RunResultStateFailed
	timedOut := databricks.RunResultStateTimedout
	errorMessage := `Task regulation_mode_driver_time_ranges-v0 failed with message: Cluster 1201-152702-6zjz069j was terminated while waiting on it to be ready: Cluster 1201-152702-6zjz069j is in unexpected state Terminated: AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE(CLIENT_ERROR): aws_api_error_code:InsufficientInstanceCapacity,aws_error_message:There is no Spot capacity available that matches your request.. This caused all downstream tasks to get skipped.`
	dbxClient.EXPECT().ListRuns(gomock.Any(), &databricks.ListRunsInput{
		JobId:       100,
		ExpandTasks: true,
		Limit:       2,
	}).Return(&databricks.ListRunsOutput{
		Runs: []*databricks.Run{
			{
				JobId: 100,
				RunId: 1,
				State: databricks.RunState{
					LifeCycleState: databricks.RunLifeCycleStateInternalError,
					ResultState:    &failed,
					StateMessage:   errorMessage,
				},
				StartTime:         now - 2*samtime.MillisecondsInHour,
				EndTime:           now - samtime.MillisecondsInHour,
				SetupDuration:     5,
				ExecutionDuration: 90,
				CleanupDuration:   5,
				RunDuration:       int(samtime.MillisecondsInHour),
				RunType:           databricks.RunTypeJobRun,
				ClusterInstance: &databricks.ClusterInstance{
					ClusterId: "100",
				},
				JobClusters: []*databricks.JobCluster{
					{
						JobClusterKey: "default",
						NewCluster:    job1clusterspec.NewCluster,
					},
				},
				Tasks: []*databricks.RunTask{
					{
						RunId:         1,
						TaskKey:       "task-1",
						AttemptNumber: 0,
						JobClusterKey: "default",
						State: databricks.RunState{
							LifeCycleState: databricks.RunLifeCycleStateInternalError,
							ResultState:    &failed,
							StateMessage:   errorMessage,
						},
						StartTime:         now - 2*samtime.MillisecondsInHour,
						EndTime:           now - samtime.MillisecondsInHour,
						SetupDuration:     5,
						ExecutionDuration: 90,
						CleanupDuration:   5,
						ClusterInstance: &databricks.ClusterInstance{
							ClusterId: "100",
						},
					},
				},
			},
			{
				JobId: 100,
				RunId: 2,
				State: databricks.RunState{
					LifeCycleState: databricks.RunLifeCycleStateTerminated,
					ResultState:    &timedOut,
				},
				StartTime:         now - 4*samtime.MillisecondsInHour,
				EndTime:           now - 2*samtime.MillisecondsInHour,
				SetupDuration:     5,
				ExecutionDuration: 90,
				CleanupDuration:   5,
				RunDuration:       int(samtime.MillisecondsInHour),
				RunType:           databricks.RunTypeJobRun,
				ClusterInstance: &databricks.ClusterInstance{
					ClusterId: "100",
				},
				JobClusters: []*databricks.JobCluster{
					{
						JobClusterKey: "default",
						NewCluster:    job1clusterspec.NewCluster,
					},
				},
				Tasks: []*databricks.RunTask{
					{
						RunId:         2,
						TaskKey:       "task-2",
						AttemptNumber: 0,
						JobClusterKey: "default",
						State: databricks.RunState{
							LifeCycleState: databricks.RunLifeCycleStateTerminated,
							ResultState:    &timedOut,
						},
						StartTime:         now - 4*samtime.MillisecondsInHour,
						EndTime:           now - 2*samtime.MillisecondsInHour,
						SetupDuration:     5,
						ExecutionDuration: 90,
						CleanupDuration:   5,
						ClusterInstance: &databricks.ClusterInstance{
							ClusterId: "100",
						},
					},
				},
			},
		},
	}, nil).Times(1)

	dbxClient.EXPECT().RunNow(gomock.Any(), &databricks.RunNowInput{
		JobId: 100,
	}).Return(&databricks.RunNowOutput{
		ErrorCode: "",
		Message:   "",
	}, nil).Times(1)

	dbxClient.EXPECT().GetRunOutput(gomock.Any(), &databricks.GetRunOutputInput{RunId: 1}).Return(&databricks.GetRunOutputOutput{
		ErrorTrace: errorMessage,
	}, nil).AnyTimes()
	err := worker.retryFailedMergeJobs(context.Background(), []databricks.Job{
		{
			JobId: 100,
			JobSettings: databricks.JobSettings{
				Name:        "job1",
				JobClusters: []*databricks.JobCluster{{NewCluster: job1clusterspec.NewCluster}},
			},
		},
	})
	require.NoError(t, err)
}
