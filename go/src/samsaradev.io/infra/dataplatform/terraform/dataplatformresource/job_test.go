package dataplatformresource_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/testloader"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

type TestCase struct {
	Name     string
	Config   *dataplatformresource.JobSpec
	Expected *databricksresource.Job
}

var nbTask1 = &databricks.NotebookTask{
	NotebookPath: "/notebook_task_1",
	BaseParameters: map[string]string{
		"start_date": "",
		"end_date":   "",
	},
	Source: databricks.DefaultTaskSource,
}

var nbTask2 = &databricks.NotebookTask{
	NotebookPath: "/notebook_task_2",
	BaseParameters: map[string]string{
		"start_date": "",
		"end_date":   "",
	},
	Source: databricks.DefaultTaskSource,
}

var spTask1 = &databricks.SparkPythonTask{
	PythonFile: "dbfs://spark_python_test.py",
	Parameters: []string{},
}

var schedule = &databricksresource.CronSchedule{
	QuartzCronExpression: "",
	TimezoneId:           databricks.TimezoneIdGMT,
}

var jobTags = map[string]string{
	"foo": "bar",
}

var js1 = dataplatformresource.JobSpec{
	Name:               "job-spec-1",
	Owner:              team.DataPlatform,
	Region:             "us-west-2",
	Profile:            "dataplatform",
	JobType:            dataplatformconsts.Workflow,
	EmailNotifications: []string{"dev-tester@samsara.com"},
	JobTags:            jobTags,
	AdminTeams: []components.TeamInfo{
		team.DataEngineering,
	},
	Cron: "0 0 0 * * ?",
	Tasks: []*databricks.JobTaskSettings{
		{
			TaskKey:      "nb-task-1",
			NotebookTask: nbTask1,
			NewCluster: &databricks.ClusterSpec{
				SparkVersion: sparkversion.SparkVersion154xScala212,
			},
		},
		{
			TaskKey:         "sp-task-2",
			SparkPythonTask: spTask1,
			JobClusterKey:   "default",
			DependsOn: []*databricks.TaskDependency{
				{
					TaskKey: "nb-task-1",
				},
			},
		},
	},
	JobClusters: []*databricks.JobCluster{
		{
			JobClusterKey: "default",
			NewCluster: &databricks.ClusterSpec{
				SparkVersion: sparkversion.SparkVersion154xScala212,
			},
		},
	},
}

var defaultSparkConfs = map[string]string{
	"spark.databricks.delta.autoCompact.enabled":                             "true",
	"spark.databricks.delta.formatCheck.enabled":                             "false",
	"spark.databricks.delta.history.metricsEnabled":                          "true",
	"spark.databricks.delta.optimizeWrite.enabled":                           "true",
	"spark.databricks.delta.properties.defaults.checkpointRetentionDuration": "INTERVAL 30 DAYS",
	"spark.databricks.delta.properties.defaults.logRetentionDuration":        "INTERVAL 30 DAYS",
	"spark.databricks.hive.metastore.client.pool.size":                       "3",
	"spark.databricks.hive.metastore.glueCatalog.enabled":                    "true",
	"spark.databricks.queryWatchdog.enabled":                                 "false",
	"spark.databricks.sql.initial.catalog.name":                              "hive_metastore",
	"spark.decommission.enabled":                                             "true",
	"spark.hadoop.aws.glue.max-error-retries":                                "10",
	"spark.hadoop.aws.glue.partition.num.segments":                           "1",
	"spark.hadoop.fs.s3a.acl.default":                                        "BucketOwnerFullControl",
	"spark.hadoop.fs.s3a.canned.acl":                                         "BucketOwnerFullControl",
	"spark.sql.shuffle.partitions":                                           "4096",
	"spark.storage.decommission.enabled":                                     "true",
	"spark.storage.decommission.rddBlocks.enabled":                           "true",
	"spark.storage.decommission.shuffleBlocks.enabled":                       "true",
}

var defaultSparkEnvVars = map[string]string{
	"AWS_DEFAULT_REGION":             "us-west-2",
	"AWS_REGION":                     "us-west-2",
	"GOOGLE_APPLICATION_CREDENTIALS": "/databricks/samsara-data-5142c7cd3ba2.json",
	"GOOGLE_CLOUD_PROJECT":           "samsara-data",
}

var defaultClusterTags = []databricksresource.ClusterTag{
	{
		Key:   "samsara:dataplatform-job-type",
		Value: "workflow",
	},
	{
		Key:   "samsara:is-production-job",
		Value: "false",
	},
	{
		Key:   "samsara:product-group",
		Value: "aianddata",
	},
	{
		Key:   "samsara:rnd-allocation",
		Value: "0",
	},
	{
		Key:   "samsara:service",
		Value: "databricksjob-job-spec-1",
	},
	{
		Key:   "samsara:team",
		Value: "dataplatform",
	},
}

var defaultClusterLogConf = &databricksresource.ClusterLogConf{
	S3: &databricksresource.S3StorageInfo{
		Destination: "s3://samsara-databricks-cluster-logs/job-spec-1",
		Region:      "us-west-2",
	},
}

var testCases = []TestCase{
	{
		Name:   "multi task with dependencies",
		Config: &js1,
		Expected: &databricksresource.Job{
			Name:         "job-spec-1-us",
			ResourceName: "job-spec-1",
			Schedule: &databricksresource.CronSchedule{
				QuartzCronExpression: "0 0 0 * * ?",
				TimezoneId:           databricks.TimezoneIdGMT,
				PauseStatus:          databricks.PauseStatusUnpaused,
			},
			EmailNotifications: &databricksresource.EmailNotifications{
				OnFailure:             []string{"dev-tester@samsara.com"},
				NoAlertForSkippedRuns: true,
			},
			Tags: map[string]string{"foo": "bar"},
			Tasks: []*databricksresource.JobTaskSettings{
				{
					TaskKey: "nb-task-1",
					NotebookTask: &databricksresource.NotebookTask{
						NotebookPath:   nbTask1.NotebookPath,
						BaseParameters: nbTask1.BaseParameters,
						Source:         databricks.DefaultTaskSource,
					},
					NewCluster: &databricksresource.Cluster{
						SparkVersion:      string(sparkversion.SparkVersion154xScala212),
						SparkConf:         defaultSparkConfs,
						SparkEnvVars:      defaultSparkEnvVars,
						NodeTypeId:        "rd-fleet.xlarge",
						DriverNodeTypeId:  "m5dn.large",
						EnableElasticDisk: true,
						NumWorkers:        pointer.IntPtr(1),
						CustomTags:        defaultClusterTags,
						ClusterLogConf:    defaultClusterLogConf,
						AwsAttributes: &databricksresource.ClusterAwsAttributes{
							FirstOnDemand:      1,
							ZoneId:             "us-west-2a",
							InstanceProfileArn: "dataplatform",
						},
						DataSecurityMode: databricksresource.DataSecurityModeNone,
					},
				},
				{
					TaskKey: "sp-task-2",
					SparkPythonTask: &databricksresource.SparkPythonTask{
						PythonFile: spTask1.PythonFile,
						Parameters: spTask1.Parameters,
					},
					JobClusterKey: "default",
					DependsOn: []*databricksresource.TaskDependency{
						{
							TaskKey: "nb-task-1",
						},
					},
				},
			},
			JobClusters: []*databricksresource.JobCluster{
				{
					JobClusterKey: "default",
					NewCluster: &databricksresource.Cluster{
						SparkVersion:      string(sparkversion.SparkVersion154xScala212),
						SparkConf:         defaultSparkConfs,
						SparkEnvVars:      defaultSparkEnvVars,
						NodeTypeId:        "rd-fleet.xlarge",
						DriverNodeTypeId:  "m5dn.large",
						EnableElasticDisk: true,
						NumWorkers:        pointer.IntPtr(1),
						CustomTags:        defaultClusterTags,
						ClusterLogConf:    defaultClusterLogConf,
						AwsAttributes: &databricksresource.ClusterAwsAttributes{
							FirstOnDemand:      1,
							ZoneId:             "us-west-2a",
							InstanceProfileArn: "dataplatform",
						},
						DataSecurityMode: databricksresource.DataSecurityModeNone,
					},
				},
			},
		},
	},
}

func TestGetJobResource(t *testing.T) {

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			job, _ := tc.Config.TfResource()
			assert.Equal(t, tc.Expected, job)
		})
	}
}

func TestGetJobSettings(t *testing.T) {
	var env struct {
		Snap *snapshotter.Snapshotter
	}
	testloader.MustStart(t, &env)

	jobName := "testJobSpec"
	jobSpec := dataplatformresource.JobSpec{
		Name:               jobName,
		Region:             "us-west-2",
		Owner:              team.DataPlatform,
		SparkVersion:       sparkversion.DefaultSparkVersion,
		MinWorkers:         1,
		MaxWorkers:         1,
		TimeoutSeconds:     int((6 * time.Hour).Seconds()),
		EmailNotifications: []string{team.DataPlatform.SlackAlertsChannelEmail.Email},
		Cron:               fmt.Sprintf("0 %d 3 * * ?", dataplatformresource.RandomFromNameHash(jobName, 0, 30)), // Everyday around 3AM UTC
		RnDCostAllocation:  1,
		JobType:            "deploy_machinery_test_job",
		SparkEnvVars: map[string]string{
			"AWS_DEFAULT_REGION": "us-west-2",
			"AWS_REGION":         "us-west-2",
		},
	}

	settings, err := jobSpec.JobSettings()
	assert.NoError(t, err)
	env.Snap.Snapshot("test_job_spec_to_job_settings", *settings)
}
