package main

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/golang/mock/gomock"
	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zorkian/go-datadog-api"

	"samsaradev.io/helpers/samtime"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/databricks/mock_databricks"
)

func TestFetchDatapipelinesMetricsServerless(t *testing.T) {
	ctx := context.Background()
	controller := gomock.NewController(t)
	dbxClient := mock_databricks.NewMockAPI(controller)
	mockClock := clock.NewMock()

	worker := Worker{
		region:           "us-west-2",
		datadogClient:    nil, // We don't need this for this test
		clock:            mockClock,
		databricksClient: dbxClient,
	}

	mockClock.Set(time.Date(2022, 01, 01, 0, 0, 0, 0, time.UTC))
	now := samtime.TimeToMs(mockClock.Now())

	success := databricks.RunResultStateSuccess
	failure := databricks.RunResultStateFailed

	datapipelinesRuns := []*databricks.Run{

		{ // Datapipeline Sqlite Node Run Success on Serverless Cluster
			RunId:     1003,
			RunType:   databricks.RunTypeSubmitRun,
			StartTime: now - 7*samtime.MillisecondsInHour,
			EndTime:   now - 6*samtime.MillisecondsInHour,
			Tasks: []*databricks.RunTask{
				{
					RunId:             1,
					StartTime:         now - 7*samtime.MillisecondsInHour,
					EndTime:           now - 6*samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 190,
					CleanupDuration:   5,
					Description:       `{"samsara:pooled-job:service": "data-pipeline-node-dataplatform_beta_report_metered_usage", "samsara:pooled-job:team": "dataplatform", "samsara:pooled-job:product-group": "AIAndData", "samsara:pipeline-node-name": "dataplatform_beta_report.metered_usage-serverless", "samsara:pooled-job:rnd-allocation": 1, "samsara:pooled-job:is-production-job": false, "samsara:pooled-job:dataplatform-job-type": "data_pipelines_sql_transformation"}`,
				},
			},
			State: databricks.RunState{
				ResultState: &success,
			},
		},
		{ // Datapipeline Transformation Node Run Failed on Serverless Cluster
			RunId:     1004,
			RunType:   databricks.RunTypeSubmitRun,
			StartTime: now - 8*samtime.MillisecondsInHour,
			EndTime:   now - 7*samtime.MillisecondsInHour,
			Tasks: []*databricks.RunTask{
				{
					RunId:             1,
					StartTime:         now - 8*samtime.MillisecondsInHour,
					EndTime:           now - 7*samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 190,
					CleanupDuration:   5,
					Description:       `{"samsara:pooled-job:service": "data-pipeline-node-dataplatform_beta_report_metered_usage", "samsara:pooled-job:team": "dataplatform", "samsara:pooled-job:product-group": "AIAndData", "samsara:pipeline-node-name": "dataplatform_beta_report.metered_usage-serverless", "samsara:pooled-job:rnd-allocation": 1, "samsara:pooled-job:is-production-job": false, "samsara:pooled-job:dataplatform-job-type": "data_pipelines_sql_transformation"}`,
				},
			},
			State: databricks.RunState{
				ResultState: &failure,
			},
		},
	}

	dbxClient.EXPECT().ListRuns(gomock.Any(), &databricks.ListRunsInput{
		CompletedOnly: true,
		StartTimeFrom: int(now - 24*samtime.MillisecondsInHour),
		ExpandTasks:   true,
		RunType:       string(databricks.RunTypeSubmitRun),
	}).Return(&databricks.ListRunsOutput{
		Runs:    datapipelinesRuns,
		HasMore: true,
	}, nil).Times(1)

	dbxClient.EXPECT().GetRunOutput(gomock.Any(), &databricks.GetRunOutputInput{
		RunId: 1004,
	}).Return(&databricks.GetRunOutputOutput{
		Error: "AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE",
	}, nil).Times(1)

	metrics, _ := worker.fetchDatapipelinesMetrics(ctx)

	metricsByJob := make(map[string][]datadog.Metric)
	for _, metric := range metrics {
		for _, tag := range metric.Tags {
			if strings.HasPrefix(tag, "node") {
				metricsByJob[tag] = append(metricsByJob[tag], metric)
				break
			}
		}
	}

	t.Run("output", func(t *testing.T) {
		// Snapshot each one with their own snapshotter to get into different files
		snap := snapshotter.New(t)
		defer snap.Verify()
		snap.Snapshot("fetchDatapipelinesMetricsServerlessTest", metricsByJob)
	})
}
func TestFetchDatapipelinesMetrics(t *testing.T) {
	ctx := context.Background()
	controller := gomock.NewController(t)
	dbxClient := mock_databricks.NewMockAPI(controller)
	mockClock := clock.NewMock()

	worker := Worker{
		region:           "us-west-2",
		datadogClient:    nil, // We don't need this for this test
		clock:            mockClock,
		databricksClient: dbxClient,
	}

	mockClock.Set(time.Date(2022, 01, 01, 0, 0, 0, 0, time.UTC))
	now := samtime.TimeToMs(mockClock.Now())

	datapipelinesTransformationNodeClusterSpec := databricks.RunClusterSpec{
		NewCluster: &databricks.ClusterSpec{
			SparkVersion: "12.2.x-scala2.12",
			CustomTags: map[string]string{
				"samsara:pipeline-node-name":               "test_report.test_node",
				"samsara:pooled-job:dataplatform-job-type": "data_pipelines_sql_transformation",
				"samsara:pooled-job:product-group":         "TestProductGroup",
				"samsara:pooled-job:service":               "data-pipeline-node-test_report-test_node-sqlite",
				"samsara:pooled-job:team":                  "owningTeam",
			},
			InstancePoolId: "pool-1",
		},
	}

	datapipelinesSqliteNodeClusterSpec := databricks.RunClusterSpec{
		NewCluster: &databricks.ClusterSpec{
			SparkVersion: "12.2.x-scala2.12",
			CustomTags: map[string]string{
				"samsara:pipeline-node-name":               "test_report.test_node-sqlite",
				"samsara:pooled-job:dataplatform-job-type": "data_pipelines_sqlite_node",
				"samsara:pooled-job:product-group":         "TestProductGroup",
				"samsara:pooled-job:service":               "data-pipeline-node-test_report-test_node-sqlite",
				"samsara:pooled-job:team":                  "owningTeam",
			},
			InstancePoolId: "pool-1",
		},
	}

	// Missing `samsara:pooled-job:dataplatform-job-type` custom tag
	nonDatapipelinesClusterSpec := databricks.RunClusterSpec{
		NewCluster: &databricks.ClusterSpec{
			SparkVersion: "12.2.x-scala2.12",
			CustomTags: map[string]string{
				"samsara:pooled-job:pool-test": "job2",
				"samsara:job:job-test":         "job2",
				"samsara:team":                 "team",
				"ignored-tag":                  "shouldnt see this",
			},
			InstancePoolId: "pool-1",
		},
	}

	// Different `samsara:pooled-job:dataplatform-job-type` custom tag
	nonDatapipelinesClusterSpec2 := databricks.RunClusterSpec{
		NewCluster: &databricks.ClusterSpec{
			SparkVersion: "12.2.x-scala2.12",
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
			InstancePoolId: "pool-1",
		},
	}

	success := databricks.RunResultStateSuccess
	failure := databricks.RunResultStateFailed

	datapipelinesRuns := []*databricks.Run{
		{ // Datapipeline Transformation Node Run Success
			RunId:     1001,
			RunType:   databricks.RunTypeSubmitRun,
			StartTime: now - 3*samtime.MillisecondsInHour,
			EndTime:   now - 2*samtime.MillisecondsInHour,
			Tasks: []*databricks.RunTask{
				{
					RunId:             1,
					NewCluster:        datapipelinesTransformationNodeClusterSpec.NewCluster,
					StartTime:         now - 3*samtime.MillisecondsInHour,
					EndTime:           now - 2*samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 190,
					CleanupDuration:   5,
				},
			},
			State: databricks.RunState{
				ResultState: &success,
			},
		},
		{ // Datapipeline Sqlite Node Run Success
			RunId:     1002,
			RunType:   databricks.RunTypeSubmitRun,
			StartTime: now - 4*samtime.MillisecondsInHour,
			EndTime:   now - 3*samtime.MillisecondsInHour,
			Tasks: []*databricks.RunTask{
				{
					RunId:             1,
					NewCluster:        datapipelinesSqliteNodeClusterSpec.NewCluster,
					StartTime:         now - 4*samtime.MillisecondsInHour,
					EndTime:           now - 3*samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 190,
					CleanupDuration:   5,
				},
			},
			State: databricks.RunState{
				ResultState: &success,
			},
		},
		{ // Datapipeline Transformation Node Run Failed
			RunId:     1003,
			RunType:   databricks.RunTypeSubmitRun,
			StartTime: now - 5*samtime.MillisecondsInHour,
			EndTime:   now - 4*samtime.MillisecondsInHour,
			Tasks: []*databricks.RunTask{
				{
					RunId:             1,
					NewCluster:        datapipelinesTransformationNodeClusterSpec.NewCluster,
					StartTime:         now - 5*samtime.MillisecondsInHour,
					EndTime:           now - 4*samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 190,
					CleanupDuration:   5,
				},
			},
			State: databricks.RunState{
				ResultState: &failure,
			},
		},
		{ // Datapipeline Sqlite Node Run Failed
			RunId:     1004,
			RunType:   databricks.RunTypeSubmitRun,
			StartTime: now - 6*samtime.MillisecondsInHour,
			EndTime:   now - 5*samtime.MillisecondsInHour,
			Tasks: []*databricks.RunTask{
				{
					RunId:             1,
					NewCluster:        datapipelinesSqliteNodeClusterSpec.NewCluster,
					StartTime:         now - 6*samtime.MillisecondsInHour,
					EndTime:           now - 5*samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 190,
					CleanupDuration:   5,
				},
			},
			State: databricks.RunState{
				ResultState: &failure,
			},
		},
	}

	nonDatapipelinesRuns := []*databricks.Run{
		{ // Non Datapipelines Submit Run
			RunId:     2001,
			RunType:   databricks.RunTypeSubmitRun,
			StartTime: now - 6*samtime.MillisecondsInHour,
			EndTime:   now - 5*samtime.MillisecondsInHour,
			Tasks: []*databricks.RunTask{
				{
					RunId:             1,
					NewCluster:        nonDatapipelinesClusterSpec.NewCluster,
					StartTime:         now - 6*samtime.MillisecondsInHour,
					EndTime:           now - 5*samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 190,
					CleanupDuration:   5,
				},
			},
			State: databricks.RunState{
				ResultState: &failure,
			},
		},
		{ // Non Datapipelines Submit Run
			RunId:     2002,
			RunType:   databricks.RunTypeSubmitRun,
			StartTime: now - 6*samtime.MillisecondsInHour,
			EndTime:   now - 5*samtime.MillisecondsInHour,
			Tasks: []*databricks.RunTask{
				{
					RunId:             1,
					NewCluster:        nonDatapipelinesClusterSpec2.NewCluster,
					StartTime:         now - 6*samtime.MillisecondsInHour,
					EndTime:           now - 5*samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 190,
					CleanupDuration:   5,
				},
			},
			State: databricks.RunState{
				ResultState: &failure,
			},
		},
	}

	variousRuns := append(nonDatapipelinesRuns, datapipelinesRuns...)

	dbxClient.EXPECT().ListRuns(gomock.Any(), &databricks.ListRunsInput{
		CompletedOnly: true,
		StartTimeFrom: int(now - 24*samtime.MillisecondsInHour),
		ExpandTasks:   true,
		RunType:       string(databricks.RunTypeSubmitRun),
	}).Return(&databricks.ListRunsOutput{
		Runs:    variousRuns,
		HasMore: true,
	}, nil).Times(1)

	dbxClient.EXPECT().GetRunOutput(gomock.Any(), &databricks.GetRunOutputInput{
		RunId: 1003,
	}).Return(&databricks.GetRunOutputOutput{
		Error: "duplicate primary key matches found.",
	}, nil).Times(1)

	dbxClient.EXPECT().GetRunOutput(gomock.Any(), &databricks.GetRunOutputInput{
		RunId: 1004,
	}).Return(&databricks.GetRunOutputOutput{
		Error: "AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE",
	}, nil).Times(1)

	metrics, _ := worker.fetchDatapipelinesMetrics(ctx)

	metricsByJob := make(map[string][]datadog.Metric)
	for _, metric := range metrics {
		for _, tag := range metric.Tags {
			if strings.HasPrefix(tag, "node") {
				metricsByJob[tag] = append(metricsByJob[tag], metric)
				break
			}
		}
	}

	t.Run("output", func(t *testing.T) {
		// Snapshot each one with their own snapshotter to get into different files
		snap := snapshotter.New(t)
		defer snap.Verify()
		snap.Snapshot("fetchDatapipelinesMetricsTest", metricsByJob)
	})
}

func TestFetchInstancePoolMetrics(t *testing.T) {
	ctx := context.Background()
	controller := gomock.NewController(t)
	dbxClient := mock_databricks.NewMockAPI(controller)
	mockClock := clock.NewMock()

	worker := Worker{
		region:           "us-west-2",
		datadogClient:    nil, // We don't need this for this test
		clock:            mockClock,
		databricksClient: dbxClient,
	}

	mockClock.Set(time.Date(2022, 01, 01, 0, 0, 0, 0, time.UTC))

	dbxClient.EXPECT().ListInstancePools(gomock.Any()).Return(&databricks.ListInstancePoolsOutput{
		InstancePools: []databricks.InstancePoolAndStats{
			{
				InstancePoolId: "pool-1",
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "test_pool_1",
					MinIdleInstances: 1,
				},
				Stats: databricks.InstancePoolStats{
					UsedCount:        5,
					IdleCount:        6,
					PendingUsedCount: 7,
					PendingIdleCount: 8,
				},
			},
			{
				InstancePoolId: "pool-2",
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "test_pool_2",
					MinIdleInstances: 2,
				},
				Stats: databricks.InstancePoolStats{
					UsedCount:        9,
					IdleCount:        10,
					PendingUsedCount: 11,
					PendingIdleCount: 12,
				},
			},
		},
	}, nil).Times(1)

	metrics, err := worker.fetchInstancePoolMetrics(ctx)
	sort.Slice(metrics, func(i, j int) bool {
		if metrics[i].GetMetric() == metrics[j].GetMetric() {
			return metrics[i].Points[0][1] < metrics[j].Points[0][1]
		}
		return metrics[i].GetMetric() < metrics[j].GetMetric()
	})
	require.NoError(t, err)

	snap := snapshotter.New(t)
	defer snap.Verify()
	snap.Snapshot("fetchInstancePoolsOutput", metrics)
}

func TestFetchJobMetrics(t *testing.T) {
	ctx := context.Background()
	controller := gomock.NewController(t)
	dbxClient := mock_databricks.NewMockAPI(controller)
	mockClock := clock.NewMock()

	worker := Worker{
		region:           "us-west-2",
		datadogClient:    nil, // We don't need this for this test
		clock:            mockClock,
		databricksClient: dbxClient,
	}

	mockClock.Set(time.Date(2022, 01, 01, 0, 0, 0, 0, time.UTC))
	now := samtime.TimeToMs(mockClock.Now())

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
		},
	}

	// pooled jobs don't have the zone id in the awsattributes, but will have
	// the pool ids filled in.
	job2clusterspec := databricks.RunClusterSpec{
		NewCluster: &databricks.ClusterSpec{
			AwsAttributes: &databricks.ClusterAwsAttributes{},
			SparkVersion:  "7.3.x-scala2.12",
			CustomTags: map[string]string{
				"samsara:pooled-job:pool-test": "job2",
				"samsara:job:job-test":         "job2",
				"samsara:team":                 "team",
				"ignored-tag":                  "shouldnt see this",
			},
			InstancePoolId: "pool-1",
		},
	}

	// jobs scheduled on interactive clusters won't have `NewCluster` but will have an existing cluster ID
	job3clusterspec := databricks.RunClusterSpec{
		ExistingClusterId: "abc-123-def",
	}
	job4clusterspec := databricks.RunClusterSpec{
		ExistingClusterId: "cluster-that-does-not-exist-anymore",
	}

	dbxClient.EXPECT().ListJobs(gomock.Any(), &databricks.ListJobsInput{ExpandTasks: true}).Return(&databricks.ListJobsOutput{
		Jobs: []databricks.Job{
			{
				JobId: 100,
				JobSettings: databricks.JobSettings{
					Name:        "test - job😬",
					JobClusters: []*databricks.JobCluster{{NewCluster: job1clusterspec.NewCluster}},
				},
			},
			{
				JobId: 200,
				JobSettings: databricks.JobSettings{
					Name:        "test job 2",
					JobClusters: []*databricks.JobCluster{{NewCluster: job2clusterspec.NewCluster}},
				},
			},
			{
				JobId: 300,
				JobSettings: databricks.JobSettings{
					Name:        "test job 3",
					JobClusters: []*databricks.JobCluster{{NewCluster: job3clusterspec.NewCluster}},
				},
			},
			{
				JobId: 400,
				JobSettings: databricks.JobSettings{
					Name:        "test job 4",
					JobClusters: []*databricks.JobCluster{{NewCluster: job3clusterspec.NewCluster}},
				},
			},
		},
	}, nil).Times(1)

	dbxClient.EXPECT().ListInstancePools(gomock.Any()).Return(&databricks.ListInstancePoolsOutput{
		InstancePools: []databricks.InstancePoolAndStats{
			{
				InstancePoolId: "pool-1",
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "my test pool",
					AwsAttributes: &databricks.InstancePoolAwsAttributes{
						ZoneId: "us-west-2c",
					},
				},
			},
		},
	}, nil).Times(1)

	dbxClient.EXPECT().ListClusters(gomock.Any(), &databricks.ListClustersInput{}).Return(&databricks.ListClustersOutput{
		Clusters: []databricks.ClusterSpec{
			{
				ClusterId:   "abc-123-def",
				ClusterName: "dataanalytics",
				CustomTags: map[string]string{
					"samsara:team":  "dataanalytics",
					"somerandomTag": "shouldNotShowUp",
				},
				AwsAttributes: &databricks.ClusterAwsAttributes{
					ZoneId: "us-west-2b",
				},
			},
		},
	}, nil).Times(1)

	success := databricks.RunResultStateSuccess
	failure := databricks.RunResultStateFailed
	canceled := databricks.RunResultStateCanceled
	timedout := databricks.RunResultStateTimedout

	runs := []*databricks.Run{
		{
			JobId: 100,
			RunId: 1,
			State: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &success,
			},
			StartTime:         now - 2*samtime.MillisecondsInHour,
			EndTime:           now - samtime.MillisecondsInHour,
			SetupDuration:     0,
			ExecutionDuration: 0,
			CleanupDuration:   0,
			RunDuration:       300,
			RunType:           databricks.RunTypeJobRun,
			Tasks: []*databricks.RunTask{
				{
					TaskKey:       "task-1",
					RunId:         1,
					AttemptNumber: 0,
					NewCluster:    job1clusterspec.NewCluster,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &success,
					},
					StartTime:         now - 2*samtime.MillisecondsInHour,
					EndTime:           now - samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 190,
					CleanupDuration:   5,
				},
				{
					TaskKey:       "task-2",
					RunId:         2,
					AttemptNumber: 0,
					NewCluster:    job1clusterspec.NewCluster,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &success,
					},
					StartTime:         now - 2*samtime.MillisecondsInHour,
					EndTime:           now - samtime.MillisecondsInHour,
					SetupDuration:     0,
					ExecutionDuration: 100,
					CleanupDuration:   0,
				},
			},
		},
		// This run is considered overrun since it started at the right time
		// but didn't end within the same hour.
		{
			JobId: 100,
			RunId: 1,
			State: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &success,
			},
			StartTime:         now - 3*samtime.MillisecondsInHour + 10*samtime.MillisecondsInMinute,
			EndTime:           now - samtime.MillisecondsInHour,
			SetupDuration:     5,
			ExecutionDuration: 90,
			CleanupDuration:   5,
			RunType:           databricks.RunTypeJobRun,
			JobClusters: []*databricks.JobCluster{
				{
					JobClusterKey: "default",
					NewCluster:    job1clusterspec.NewCluster,
				},
			},
			Tasks: []*databricks.RunTask{
				{
					TaskKey:       "task-1",
					RunId:         1,
					AttemptNumber: 0,
					JobClusterKey: "default",
					NewCluster:    job1clusterspec.NewCluster,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &success,
					},
					StartTime:         now - 3*samtime.MillisecondsInHour + 10*samtime.MillisecondsInMinute,
					EndTime:           now - samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 90,
					CleanupDuration:   5,
				},
			},
		},
		// Case: task retry succeeds
		{
			JobId: 100,
			RunId: 1,
			State: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &success,
			},
			StartTime:         now - 4*samtime.MillisecondsInHour,
			EndTime:           now - samtime.MillisecondsInHour,
			SetupDuration:     0,
			ExecutionDuration: 0,
			CleanupDuration:   0,
			RunDuration:       190,
			RunType:           databricks.RunTypeJobRun,
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
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &failure,
					},
					StartTime:         now - 4*samtime.MillisecondsInHour,
					EndTime:           now - samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 90,
					CleanupDuration:   0,
				},
				{
					RunId:         2,
					TaskKey:       "task-1",
					AttemptNumber: 1,
					JobClusterKey: "default",
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &success,
					},
					StartTime:         now - 2*samtime.MillisecondsInHour,
					EndTime:           now - samtime.MillisecondsInHour,
					SetupDuration:     0,
					ExecutionDuration: 90,
					CleanupDuration:   5,
				},
			},
		},
		{
			JobId: 200,
			RunId: 1,
			State: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &timedout,
			},
			StartTime:         now - 5*samtime.MillisecondsInHour,
			EndTime:           now - samtime.MillisecondsInHour,
			SetupDuration:     5,
			ExecutionDuration: 190,
			CleanupDuration:   5,
			RunType:           databricks.RunTypeJobRun,
			Tasks: []*databricks.RunTask{
				{
					RunId:      1,
					NewCluster: job2clusterspec.NewCluster,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &timedout,
					},
					StartTime:         now - 5*samtime.MillisecondsInHour,
					EndTime:           now - samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 190,
					CleanupDuration:   5,
				},
			},
		},
		{
			RunId: 1,
			JobId: 200,
			State: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &canceled,
			},
			StartTime:         now - 6*samtime.MillisecondsInHour,
			EndTime:           now - samtime.MillisecondsInHour,
			SetupDuration:     5,
			ExecutionDuration: 90,
			CleanupDuration:   5,
			RunType:           databricks.RunTypeJobRun,
			Tasks: []*databricks.RunTask{
				{
					RunId:      1,
					NewCluster: job2clusterspec.NewCluster,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &canceled,
					},
					StartTime:         now - 6*samtime.MillisecondsInHour,
					EndTime:           now - samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 90,
					CleanupDuration:   5,
				},
			},
		},
		{
			RunId: 1,
			JobId: 200,
			State: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &failure,
			},
			StartTime:         now - 7*samtime.MillisecondsInHour,
			EndTime:           now - samtime.MillisecondsInHour,
			SetupDuration:     5,
			ExecutionDuration: 90,
			CleanupDuration:   5,
			RunType:           databricks.RunTypeJobRun,
			Tasks: []*databricks.RunTask{
				{
					RunId:      1,
					NewCluster: job2clusterspec.NewCluster,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &failure,
					},
					StartTime:         now - 7*samtime.MillisecondsInHour,
					EndTime:           now - samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 90,
					CleanupDuration:   5,
				},
			},
		},
		{
			RunId: 1,
			JobId: 300,
			State: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &timedout,
			},
			StartTime:         now - 5*samtime.MillisecondsInHour,
			EndTime:           now - samtime.MillisecondsInHour,
			SetupDuration:     5,
			ExecutionDuration: 190,
			CleanupDuration:   5,
			RunType:           databricks.RunTypeJobRun,
			Tasks: []*databricks.RunTask{
				{
					RunId:      1,
					NewCluster: job3clusterspec.NewCluster,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &timedout,
					},
					StartTime:         now - 5*samtime.MillisecondsInHour,
					EndTime:           now - samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 190,
					CleanupDuration:   5,
					ExistingClusterId: job3clusterspec.ExistingClusterId,
				},
			},
		},
		{
			RunId: 1,
			JobId: 300,
			State: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &canceled,
			},
			StartTime:         now - 6*samtime.MillisecondsInHour,
			EndTime:           now - samtime.MillisecondsInHour,
			SetupDuration:     5,
			ExecutionDuration: 90,
			CleanupDuration:   5,
			RunType:           databricks.RunTypeJobRun,
			Tasks: []*databricks.RunTask{
				{
					RunId:      1,
					NewCluster: job3clusterspec.NewCluster,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &canceled,
					},
					StartTime:         now - 6*samtime.MillisecondsInHour,
					EndTime:           now - samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 90,
					CleanupDuration:   5,
					ExistingClusterId: job3clusterspec.ExistingClusterId,
				},
			},
		},
		// Add a single job that is on an interactive cluster where we can't find the cluster name based on ID
		{
			RunId: 1,
			JobId: 400,
			State: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &canceled,
			},
			StartTime:         now - 6*samtime.MillisecondsInHour,
			EndTime:           now - samtime.MillisecondsInHour,
			SetupDuration:     5,
			ExecutionDuration: 90,
			CleanupDuration:   5,
			RunType:           databricks.RunTypeJobRun,
			Tasks: []*databricks.RunTask{
				{
					RunId: 1,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &canceled,
					},
					StartTime:         now - 6*samtime.MillisecondsInHour,
					EndTime:           now - samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 90,
					CleanupDuration:   5,
					ExistingClusterId: job4clusterspec.ExistingClusterId,
				},
			},
			JobClusters: []*databricks.JobCluster{
				{
					JobClusterKey: "default",
					NewCluster:    job1clusterspec.NewCluster,
				},
			},
		},
		{
			RunId: 1,
			JobId: 300,
			State: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &failure,
			},
			StartTime:         now - 52*samtime.MillisecondsInHour, // after seeing this the worker should stop
			EndTime:           now - samtime.MillisecondsInHour,
			SetupDuration:     5,
			ExecutionDuration: 90,
			CleanupDuration:   5,
			RunType:           databricks.RunTypeJobRun,
			Tasks: []*databricks.RunTask{
				{
					RunId:      1,
					NewCluster: job3clusterspec.NewCluster,
					State: databricks.RunState{
						LifeCycleState: databricks.RunLifeCycleStateTerminated,
						ResultState:    &failure,
					},
					StartTime:         now - 52*samtime.MillisecondsInHour, // after seeing this the worker should stop
					EndTime:           now - samtime.MillisecondsInHour,
					SetupDuration:     5,
					ExecutionDuration: 90,
					CleanupDuration:   5,
					ExistingClusterId: job3clusterspec.ExistingClusterId,
				},
			},
		},
	}

	// Add one more run that is returned the LIST endpoint, but NOT be returned by GetRun;
	// this simulates a run that has been deleted while we are iterating through runs

	runThatDoesNotExist := &databricks.Run{
		JobId: 9876,
		RunId: 54321,
		State: databricks.RunState{
			LifeCycleState: databricks.RunLifeCycleStateTerminated,
			ResultState:    &success,
		},
		StartTime:         now - 2*samtime.MillisecondsInHour,
		EndTime:           now - samtime.MillisecondsInHour,
		SetupDuration:     0,
		ExecutionDuration: 0,
		CleanupDuration:   0,
		RunDuration:       300,
		RunType:           databricks.RunTypeJobRun,
		Tasks: []*databricks.RunTask{
			{
				TaskKey:       "task-1",
				RunId:         1,
				AttemptNumber: 0,
				NewCluster:    job1clusterspec.NewCluster,
				State: databricks.RunState{
					LifeCycleState: databricks.RunLifeCycleStateTerminated,
					ResultState:    &success,
				},
				StartTime:         now - 2*samtime.MillisecondsInHour,
				EndTime:           now - samtime.MillisecondsInHour,
				SetupDuration:     5,
				ExecutionDuration: 190,
				CleanupDuration:   5,
			},
		}}
	runsWithExtraRunThatDoesNotExist := append(runs, runThatDoesNotExist)

	// Set up a few runs of each job type
	dbxClient.EXPECT().ListRuns(gomock.Any(), &databricks.ListRunsInput{
		ActiveOnly: true,
	}).Return(&databricks.ListRunsOutput{
		Runs:    runsWithExtraRunThatDoesNotExist,
		HasMore: true,
	}, nil).Times(1)

	// Set up a few runs of each job type
	dbxClient.EXPECT().ListRuns(gomock.Any(), &databricks.ListRunsInput{
		CompletedOnly: true,
		StartTimeFrom: 1640822400000,
		ExpandTasks:   true,
		RunType:       string(databricks.RunTypeJobRun),
	}).Return(&databricks.ListRunsOutput{
		Runs:    runsWithExtraRunThatDoesNotExist,
		HasMore: true,
	}, nil).Times(1)

	dbxClient.EXPECT().ListClustersV2(gomock.Any(), &databricks.ListClustersInputV2{FilterBy: databricks.FilterBy{
		ClusterStates: []string{"PENDING", "RUNNING"},
	}}).Return(&databricks.ListClustersOutputV2{
		Clusters: []databricks.ClusterSpec{
			{
				ClusterId:   "abc-123-def",
				ClusterName: "dataanalytics",
				CustomTags: map[string]string{
					"samsara:team":  "dataanalytics",
					"somerandomTag": "shouldNotShowUp",
				},
				AwsAttributes: &databricks.ClusterAwsAttributes{
					ZoneId: "us-west-2b",
				},
			},
			{
				ClusterId:   "abc-456-def",
				ClusterName: "dataplatform",
				CustomTags: map[string]string{
					"samsara:team":  "dataplatform",
					"somerandomTag": "shouldNotShowUp",
				},
				AwsAttributes: &databricks.ClusterAwsAttributes{
					ZoneId: "us-west-2c",
				},
			},
		},
	}, nil).Times(1)

	metrics, err := worker.fetchJobMetrics(ctx)
	require.NoError(t, err)

	// Sort metrics by job just to clean up the snapshot slightly
	metricsByJob := make(map[string][]datadog.Metric)
	for _, metric := range metrics {
		foundTag := false
		for _, tag := range metric.Tags {
			if strings.HasPrefix(tag, "job_id") {
				metricsByJob[tag] = append(metricsByJob[tag], metric)
				foundTag = true
				break
			}
		}
		if !foundTag {
			metricsByJob["other"] = append(metricsByJob["other"], metric)
		}
	}

	var jobIds []string
	for jobId := range metricsByJob {
		jobIds = append(jobIds, jobId)
	}
	sort.Strings(jobIds)

	for _, jobId := range jobIds {
		t.Run(""+jobId, func(t *testing.T) {
			// Snapshot each one with their own snapshotter to get into different files
			snap := snapshotter.New(t)
			defer snap.Verify()
			snap.Snapshot(jobId, metricsByJob[jobId])
		})
	}
}

func TestCleanName(t *testing.T) {
	longJobName := "test-job  with very long \t name and weird space and stuff "
	for i := 0; i < 100; i++ {
		longJobName += "abcdefghijklmnopqrstuvwxyz"
	}

	tests := []struct {
		description    string
		input          string
		expectedOutput string
	}{
		{
			description:    "basic",
			input:          " 	normal job name   ",
			expectedOutput: "normal_job_name",
		},
		{
			description:    "with illegal chars",
			input:          "😬hell!0_[     o!k    ]job",
			expectedOutput: "hell0_[_ok_]job",
		},
		{
			description:    "very long",
			input:          longJobName,
			expectedOutput: "test-job_with_very_long_name_and_weird_space_and_stuff_abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzow7lf",
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			assert.Equal(t, test.expectedOutput, cleanName(test.input))
		})
	}
}

func TestClassifyRunStatus(t *testing.T) {

	runResultStateFailed := databricks.RunResultStateFailed
	runResultStateSuccess := databricks.RunResultStateSuccess
	runResultStateSuccessWithFailures := databricks.RunResultStateSuccessWithFailures
	runResultStateTimedout := databricks.RunResultStateTimedout
	runResultStateCanceled := databricks.RunResultStateCanceled
	runResultStateUnknown := databricks.RunResultState("foo")

	tests := []struct {
		description    string
		input          databricks.RunState
		expectedOutput RunStatus
	}{
		{
			description: "lifecycle_terminated_runstate_nonexists",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    nil,
			},
			expectedOutput: RunStatus{
				Status:      "unknown",
				FailureType: "",
			},
		},
		{
			description: "lifecycle_terminated_runstate_failed",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &runResultStateFailed,
			},
			expectedOutput: RunStatus{
				Status:      "failure",
				FailureType: "failed",
			},
		},
		{
			description: "lifecycle_terminated_runstate_timedout",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &runResultStateTimedout,
			},
			expectedOutput: RunStatus{
				Status:      "failure",
				FailureType: "timedout",
			},
		},
		{
			description: "lifecycle_terminated_runstate_canceled",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &runResultStateCanceled,
			},
			expectedOutput: RunStatus{
				Status:      "failure",
				FailureType: "canceled",
			},
		},
		{
			description: "success_with_failures",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &runResultStateSuccessWithFailures,
			},
			expectedOutput: RunStatus{
				Status:      "failure",
				FailureType: "success_with_failures",
			},
		},
		{
			description: "lifecycle_terminated_runstate_success",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminated,
				ResultState:    &runResultStateSuccess,
			},
			expectedOutput: RunStatus{
				Status:      "success",
				FailureType: "",
			},
		},
		{
			description: "lifecycle_internalerror_runstate_noexists",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateInternalError,
				ResultState:    nil,
			},
			expectedOutput: RunStatus{
				Status:      "error",
				FailureType: "internal",
			},
		},
		{
			description: "lifecycle_internalerror_runstate_failed",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateInternalError,
				ResultState:    &runResultStateFailed,
			},
			expectedOutput: RunStatus{
				Status:      "failure",
				FailureType: "failed",
			},
		},
		{
			description: "lifecycle_internalerror_runstate_timedout",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateInternalError,
				ResultState:    &runResultStateTimedout,
			},
			expectedOutput: RunStatus{
				Status:      "failure",
				FailureType: "timedout",
			},
		},
		{
			description: "lifecycle_internalerror_runstate_unknown",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateInternalError,
				ResultState:    &runResultStateUnknown,
			},
			expectedOutput: RunStatus{
				Status:      "failure",
				FailureType: "internal",
			},
		},
		{
			description: "lifecycle_skipped",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateSkipped,
			},
			expectedOutput: RunStatus{
				Status: "skipped",
			},
		},
		{
			description: "lifecycle_pending",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStatePending,
			},
			expectedOutput: RunStatus{
				Status: "waiting",
			},
		},
		{
			description: "lifecycle_blocked",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateBlocked,
			},
			expectedOutput: RunStatus{
				Status: "waiting",
			},
		},
		{
			description: "lifecycle_waiting",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateWaitingForRetry,
			},
			expectedOutput: RunStatus{
				Status: "waiting",
			},
		},
		{
			description: "lifecycle_running",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateRunning,
			},
			expectedOutput: RunStatus{
				Status: "running",
			},
		},
		{
			description: "lifecycle_terminating",
			input: databricks.RunState{
				LifeCycleState: databricks.RunLifeCycleStateTerminating,
			},
			expectedOutput: RunStatus{
				Status: "running",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			actualRunStatus, _ := classifyRunStatus(test.input)
			assert.Equal(t, test.expectedOutput, actualRunStatus)
		})
	}
}

func TestGetTaskRunCluster(t *testing.T) {

	defaultExistingCluster := databricks.ClusterSpec{
		ClusterName:  "dataplatform",
		SparkVersion: "7.3.x-scala2.12",
		ClusterId:    "abc-123-def",
	}

	existingClusters := map[string]databricks.ClusterSpec{
		"abc-123-def": defaultExistingCluster,
	}

	newCluster := databricks.ClusterSpec{
		SparkVersion: "12.2.x-scala2.12",
	}

	defaultJobCluster := databricks.JobCluster{
		JobClusterKey: "",
		NewCluster: &databricks.ClusterSpec{
			SparkVersion: "10.4.x-scala2.12",
		},
	}

	jobClusters := map[string]*databricks.JobCluster{
		"default": &defaultJobCluster,
	}

	tests := []struct {
		description    string
		input          databricks.RunTask
		expectedOutput *databricks.ClusterSpec
	}{
		{
			description: "task_run_on_taskcluster",
			input: databricks.RunTask{
				NewCluster: &newCluster,
			},
			expectedOutput: &newCluster,
		},
		{
			description: "task_run_on_jobcluster",
			input: databricks.RunTask{
				JobClusterKey: "default",
				NewCluster:    &newCluster,
			},
			expectedOutput: defaultJobCluster.NewCluster,
		},
		{
			description: "task_run_on_existingcluster",
			input: databricks.RunTask{
				ExistingClusterId: "abc-123-def",
				JobClusterKey:     "default",
			},
			expectedOutput: &defaultExistingCluster,
		},
		{
			description: "task_run_on_existingcluster_noexists",
			input: databricks.RunTask{
				ExistingClusterId: "does-not-exist",
			},
			expectedOutput: nil,
		},
		{
			description: "task_run_on_existingcluster_noexists_with_fallback",
			input: databricks.RunTask{
				ExistingClusterId: "does-not-exist",
				JobClusterKey:     "default",
			},
			expectedOutput: defaultJobCluster.NewCluster,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			ctx := context.Background()

			actualTaskRunCluster := getTaskRunCluster(ctx, test.input, jobClusters, existingClusters)
			assert.Equal(t, test.expectedOutput, actualTaskRunCluster)
		})
	}
}

func TestWorker_parseCustomDatapipelineTags(t *testing.T) {
	type fields struct {
		region           string
		datadogClient    *datadog.Client
		clock            clock.Clock
		databricksClient databricks.API
	}
	type args struct {
		jsonStr string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    map[string]string
		wantErr bool
	}{
		{
			name: "basic",
			args: args{
				jsonStr: `{"samsara:pooled-job:service": "data-pipeline-node-dataplatform_beta_report_metered_usage", "samsara:pooled-job:team": "dataplatform", "samsara:pooled-job:product-group": "AIAndData", "samsara:pipeline-node-name": "dataplatform_beta_report.metered_usage", "samsara:pooled-job:rnd-allocation": 1, "samsara:pooled-job:is-production-job": false, "samsara:pooled-job:dataplatform-job-type": "data_pipelines_sql_transformation"}`,
			},
			want: map[string]string{
				"samsara:pooled-job:service":               "data-pipeline-node-dataplatform_beta_report_metered_usage",
				"samsara:pooled-job:team":                  "dataplatform",
				"samsara:pooled-job:product-group":         "AIAndData",
				"samsara:pipeline-node-name":               "dataplatform_beta_report.metered_usage",
				"samsara:pooled-job:rnd-allocation":        "1",
				"samsara:pooled-job:is-production-job":     "false",
				"samsara:pooled-job:dataplatform-job-type": "data_pipelines_sql_transformation",
			},
			wantErr: false,
		},
		{
			name: "invalid_json",
			args: args{
				jsonStr: `{"samsara:pooled-job:service"}`,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{
				region:           tt.fields.region,
				datadogClient:    tt.fields.datadogClient,
				clock:            tt.fields.clock,
				databricksClient: tt.fields.databricksClient,
			}
			got, err := w.parseCustomDatapipelineTags(tt.args.jsonStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Worker.parseCustomDatapipelineTags() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Worker.parseCustomDatapipelineTags() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getSamsaraTags(t *testing.T) {
	type args struct {
		ctx         context.Context
		poolsById   map[string]databricks.InstancePoolAndStats
		clusterSpec *databricks.ClusterSpec
		jobTags     map[string]string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "classic_cluster",
			args: args{
				ctx:       context.Background(),
				poolsById: nil,
				clusterSpec: &databricks.ClusterSpec{
					CustomTags: map[string]string{
						"samsara:team": "dataplatform",
					},
				},
				jobTags: nil,
			},
			want: []string{
				"team:dataplatform",
				"availability_zone:unknown",
				"serverless:false",
			},
		},
		{
			name: "serverless_cluster",
			args: args{
				ctx:         context.Background(),
				poolsById:   nil,
				clusterSpec: nil,
				jobTags: map[string]string{
					"samsara:team": "dataplatform",
				},
			},
			want: []string{
				"team:dataplatform",
				"serverless:true",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getSamsaraTags(tt.args.ctx, tt.args.poolsById, tt.args.clusterSpec, tt.args.jobTags); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getSamsaraTags() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFetchJobMetricsTimeSinceLastSuccess(t *testing.T) {
	mockClock := clock.NewMock()
	mockClock.Set(time.Date(2022, 01, 01, 0, 0, 0, 0, time.UTC))
	now := samtime.TimeToMs(mockClock.Now())

	ctx := context.Background()
	controller := gomock.NewController(t)
	dbxClient := mock_databricks.NewMockAPI(controller)

	worker := Worker{
		region:           "us-west-2",
		datadogClient:    nil, // We don't need this for this test
		clock:            mockClock,
		databricksClient: dbxClient,
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
		},
	}

	jobs := []databricks.Job{
		databricks.Job {
			JobId: 600,
			JobSettings: databricks.JobSettings{
				Name:        "Job has only failed runs",
				JobClusters: []*databricks.JobCluster{{NewCluster: job1clusterspec.NewCluster}},
			},
		},
		databricks.Job {
			JobId: 700,
			JobSettings: databricks.JobSettings{
				Name:        "Job with no runs",
				JobClusters: []*databricks.JobCluster{{NewCluster: job1clusterspec.NewCluster}},
			},
		},
		databricks.Job {
			JobId: 800,
			JobSettings: databricks.JobSettings{
				Name:        "Job with mix of failures and successes",
				JobClusters: []*databricks.JobCluster{{NewCluster: job1clusterspec.NewCluster}},
			},
		},
	}

	failure := databricks.RunResultStateFailed
	success := databricks.RunResultStateSuccess

	job600Run1 := databricks.Run {
		JobId: jobs[0].JobId,
		RunId: 1,
		State: databricks.RunState{
			LifeCycleState: databricks.RunLifeCycleStateTerminated,
			ResultState:    &failure,
		},
		StartTime:         now - 2*samtime.MillisecondsInHour,
		EndTime:           now - samtime.MillisecondsInHour,
		SetupDuration:     0,
		ExecutionDuration: 0,
		CleanupDuration:   0,
		RunDuration:       300,
		RunType:           databricks.RunTypeJobRun,
		Tasks: []*databricks.RunTask{
			{
				TaskKey:       "task-1",
				RunId:         1,
				AttemptNumber: 0,
				NewCluster:    job1clusterspec.NewCluster,
				State: databricks.RunState{
					LifeCycleState: databricks.RunLifeCycleStateTerminated,
					ResultState:    &failure,
				},
				StartTime:         now - 2*samtime.MillisecondsInHour,
				EndTime:           now - samtime.MillisecondsInHour,
				SetupDuration:     5,
				ExecutionDuration: 190,
				CleanupDuration:   5,
			},
			{
				TaskKey:       "task-2",
				RunId:         2,
				AttemptNumber: 0,
				NewCluster:    job1clusterspec.NewCluster,
				State: databricks.RunState{
					LifeCycleState: databricks.RunLifeCycleStateTerminated,
					ResultState:    &failure,
				},
				StartTime:         now - 2*samtime.MillisecondsInHour,
				EndTime:           now - samtime.MillisecondsInHour,
				SetupDuration:     0,
				ExecutionDuration: 100,
				CleanupDuration:   0,
			},
		},
	}

	job700Run1 := databricks.Run {
		JobId: jobs[2].JobId,
		RunId: 1,
		State: databricks.RunState{
			LifeCycleState: databricks.RunLifeCycleStateTerminated,
			ResultState:    &success,
		},
		StartTime:         now - 2*samtime.MillisecondsInHour,
		EndTime:           now - samtime.MillisecondsInHour,
		SetupDuration:     0,
		ExecutionDuration: 0,
		CleanupDuration:   0,
		RunDuration:       300,
		RunType:           databricks.RunTypeJobRun,
		Tasks: []*databricks.RunTask{
			{
				TaskKey:       "task-1",
				RunId:         1,
				AttemptNumber: 0,
				NewCluster:    job1clusterspec.NewCluster,
				State: databricks.RunState{
					LifeCycleState: databricks.RunLifeCycleStateTerminated,
					ResultState:    &success,
				},
				StartTime:         now - 2*samtime.MillisecondsInHour,
				EndTime:           now - samtime.MillisecondsInHour,
				SetupDuration:     5,
				ExecutionDuration: 190,
				CleanupDuration:   5,
			},
		},
	}

	job700Run2 := databricks.Run {
		JobId: jobs[2].JobId,
		RunId: 1,
		State: databricks.RunState{
			LifeCycleState: databricks.RunLifeCycleStateTerminated,
			ResultState:    &failure,
		},
		StartTime:         now - 2*samtime.MillisecondsInHour,
		EndTime:           now - samtime.MillisecondsInHour,
		SetupDuration:     0,
		ExecutionDuration: 0,
		CleanupDuration:   0,
		RunDuration:       300,
		RunType:           databricks.RunTypeJobRun,
		Tasks: []*databricks.RunTask{
			{
				TaskKey:       "task-1",
				RunId:         1,
				AttemptNumber: 0,
				NewCluster:    job1clusterspec.NewCluster,
				State: databricks.RunState{
					LifeCycleState: databricks.RunLifeCycleStateTerminated,
					ResultState:    &failure,
				},
				StartTime:         now - 2*samtime.MillisecondsInHour,
				EndTime:           now - samtime.MillisecondsInHour,
				SetupDuration:     5,
				ExecutionDuration: 190,
				CleanupDuration:   5,
			},
		},
	}

	// Mocks call metrics.go:344
	dbxClient.EXPECT().ListJobs(gomock.Any(), &databricks.ListJobsInput{ExpandTasks: true}).Return(&databricks.ListJobsOutput{
		Jobs: jobs,
	}, nil).Times(1)

	// Mocks call metrics.go:352
	dbxClient.EXPECT().ListInstancePools(gomock.Any()).Return(&databricks.ListInstancePoolsOutput{
		InstancePools: []databricks.InstancePoolAndStats{
			{
				InstancePoolId: "pool-1",
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "my test pool",
					AwsAttributes: &databricks.InstancePoolAwsAttributes{
						ZoneId: "us-west-2c",
					},
				},
			},
		},
	}, nil).Times(1)

	// Mocks call metrics.go:363
	dbxClient.EXPECT().ListClusters(gomock.Any(), &databricks.ListClustersInput{}).Return(&databricks.ListClustersOutput{
		Clusters: []databricks.ClusterSpec{
			{
				ClusterId:   "abc-123-def",
				ClusterName: "dataanalytics",
				CustomTags: map[string]string{
					"samsara:team":  "dataanalytics",
				},
				AwsAttributes: &databricks.ClusterAwsAttributes{
					ZoneId: "us-west-2b",
				},
			},
		},
	}, nil).Times(1)

	// Mocks call metrics.go:456
	dbxClient.EXPECT().ListRuns(gomock.Any(), &databricks.ListRunsInput{
		ActiveOnly: true,
	}).Return(&databricks.ListRunsOutput{
		Runs:    []*databricks.Run {},
		HasMore: true,
	}, nil).Times(1)

	// Mocks call metrics.go:470
	dbxClient.EXPECT().ListClustersV2(gomock.Any(), &databricks.ListClustersInputV2{FilterBy: databricks.FilterBy{
		ClusterStates: []string{"PENDING", "RUNNING"},
	}}).Return(&databricks.ListClustersOutputV2{
		Clusters: []databricks.ClusterSpec{
			{
				ClusterId:   "abc-123-def",
				ClusterName: "dataanalytics",
				CustomTags: map[string]string{
					"samsara:team":  "dataanalytics",
					"somerandomTag": "shouldNotShowUp",
				},
				AwsAttributes: &databricks.ClusterAwsAttributes{
					ZoneId: "us-west-2b",
				},
			},
			{
				ClusterId:   "abc-456-def",
				ClusterName: "dataplatform",
				CustomTags: map[string]string{
					"samsara:team":  "dataplatform",
					"somerandomTag": "shouldNotShowUp",
				},
				AwsAttributes: &databricks.ClusterAwsAttributes{
					ZoneId: "us-west-2c",
				},
			},
		},
	}, nil).Times(1)

	// Mocks call metrics.go:497
	dbxClient.EXPECT().ListRuns(gomock.Any(), &databricks.ListRunsInput{
		CompletedOnly: true,
		StartTimeFrom: int(now - 48 * samtime.MillisecondsInHour),
		ExpandTasks:   true,
		RunType:       string(databricks.RunTypeJobRun),
	}).Return(&databricks.ListRunsOutput{
		Runs:    []*databricks.Run{&job600Run1, &job700Run1, &job700Run2},
		HasMore: true,
	}, nil).Times(1)

	// Mocks call metrics.go:637
	dbxClient.EXPECT().ListRuns(gomock.Any(), &databricks.ListRunsInput{
		JobId: jobs[0].JobId,
		CompletedOnly: true,
	}).Return(&databricks.ListRunsOutput{
		Runs:    []*databricks.Run {&job600Run1},
		HasMore: true,
	}, nil).Times(1)

	dbxClient.EXPECT().ListRuns(gomock.Any(), &databricks.ListRunsInput{
		JobId: jobs[1].JobId,
		CompletedOnly: true,
	}).Return(&databricks.ListRunsOutput{
		Runs:    []*databricks.Run {},
		HasMore: true,
	}, nil).Times(1)

	metrics, err := worker.fetchJobMetrics(ctx)
	require.NoError(t, err)

	// Sort metrics by job just to clean up the snapshot slightly
	metricsByJob := make(map[string][]datadog.Metric)
	for _, metric := range metrics {
		foundTag := false
		for _, tag := range metric.Tags {
			if strings.HasPrefix(tag, "job_id") {
				metricsByJob[tag] = append(metricsByJob[tag], metric)
				foundTag = true
				break
			}
		}
		if !foundTag {
			metricsByJob["other"] = append(metricsByJob["other"], metric)
		}
	}

	var jobIds []string
	for jobId := range metricsByJob {
		jobIds = append(jobIds, jobId)
	}
	sort.Strings(jobIds)

	for _, jobId := range jobIds {
		t.Run(""+jobId, func(t *testing.T) {
			// Snapshot each one with their own snapshotter to get into different files
			snap := snapshotter.New(t)
			defer snap.Verify()
			snap.Snapshot(jobId, metricsByJob[jobId])
		})
	}
}
