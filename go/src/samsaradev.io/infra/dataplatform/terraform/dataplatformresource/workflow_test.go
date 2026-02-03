package dataplatformresource

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/ni/sparkversion"
)

type TestCase struct {
	Name     string
	Config   WorkflowScheduleConfig
	Expected []*databricks.JobCluster
}

var testCases = []TestCase{
	{
		Name: "test no override",
		Config: WorkflowScheduleConfig{
			Workflow: &databricks.JobSettings{
				JobClusters: []*databricks.JobCluster{
					{
						JobClusterKey: "jc1",
						NewCluster: &databricks.ClusterSpec{
							SparkVersion: sparkversion.SparkVersion154xScala212,
						},
					},
				},
			},
		},
		Expected: []*databricks.JobCluster{
			{
				JobClusterKey: "jc1",
				NewCluster: &databricks.ClusterSpec{
					SparkVersion: sparkversion.SparkVersion154xScala212,
				},
			},
		},
	},
	{
		Name: "test with override",
		Config: WorkflowScheduleConfig{
			Workflow: &databricks.JobSettings{
				JobClusters: []*databricks.JobCluster{
					{
						JobClusterKey: "jc1",
						NewCluster: &databricks.ClusterSpec{
							SparkVersion: sparkversion.SparkVersion143xScala212,
						},
					},
				},
			}, WorkflowOverrides: &WorkflowOverrides{
				JobClusters: []*databricks.JobCluster{
					{
						JobClusterKey: "jc1",
						NewCluster: &databricks.ClusterSpec{
							SparkVersion: sparkversion.SparkVersion154xScala212,
						},
					},
				},
			},
		},
		Expected: []*databricks.JobCluster{
			{
				JobClusterKey: "jc1",
				NewCluster: &databricks.ClusterSpec{
					SparkVersion: sparkversion.SparkVersion154xScala212,
				},
			},
		},
	},
	{
		Name: "test with no matching override",
		Config: WorkflowScheduleConfig{
			Workflow: &databricks.JobSettings{
				JobClusters: []*databricks.JobCluster{
					{
						JobClusterKey: "jc1",
						NewCluster: &databricks.ClusterSpec{
							SparkVersion: sparkversion.SparkVersion143xScala212,
						},
					},
				},
			}, WorkflowOverrides: &WorkflowOverrides{
				JobClusters: []*databricks.JobCluster{
					{
						JobClusterKey: "jc2",
						NewCluster: &databricks.ClusterSpec{
							SparkVersion: sparkversion.SparkVersion154xScala212,
						},
					},
				},
			},
		},
		Expected: nil,
	},
	{
		Name: "test undeclared input",
		Config: WorkflowScheduleConfig{
			Workflow: &databricks.JobSettings{
				Name: "test undeclared input",
			},
		},
		Expected: nil,
	},
	{
		Name: "test nil input",
		Config: WorkflowScheduleConfig{
			Workflow: &databricks.JobSettings{
				JobClusters: nil,
			},
		},
		Expected: nil,
	},
	{
		Name: "test empty input",
		Config: WorkflowScheduleConfig{
			Workflow: &databricks.JobSettings{
				JobClusters: []*databricks.JobCluster{},
			},
		},
		Expected: nil,
	},
}

func TestGenerateJobClusters(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			jc, _ := generateJobClusters(tc.Config)
			assert.Equal(t, tc.Expected, jc)
		})
	}
}
