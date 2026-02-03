package databricksdeploymachineryregistry_test

import (
	"context"
	"testing"

	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/databricks/mock_databricks"
	"samsaradev.io/infra/dataplatform/databricksdeploymachineryregistry"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/testloader"
)

func TestS3BigstatFetchDynamicFieldsForJobs(t *testing.T) {
	ctx := context.Background()
	var env struct {
		DatabricksClient *mock_databricks.MockAPI
		Snap             *snapshotter.Snapshotter
	}
	testloader.MustStart(t, &env)

	config, err := dataplatformconfig.DatabricksProviderConfig("databricks-dev-us")
	assert.NoError(t, err)

	s3bigstats := databricksdeploymachineryregistry.NewS3BigStatsDeployMachineryPipeline()
	jobs, err := s3bigstats.AddJobsToDeploMachineryRegistry(config)
	assert.NoError(t, err)
	jobSettings := make([]*databricks.JobSettings, len(jobs))
	for _, job := range jobs {
		jobSetting, err := job.DatabricksJobSpec.JobSettings()
		assert.NoError(t, err)
		jobSettings = append(jobSettings, jobSetting)
	}

	euConfig, err := dataplatformconfig.DatabricksProviderConfig("databricks-dev-eu")
	assert.NoError(t, err)

	euJobs, err := s3bigstats.AddJobsToDeploMachineryRegistry(euConfig)
	assert.NoError(t, err)
	for _, job := range euJobs {
		jobSetting, err := job.DatabricksJobSpec.JobSettings()
		assert.NoError(t, err)
		jobSettings = append(jobSettings, jobSetting)
	}

	// We want this snapshot to update if any of the fields change
	// If any of the fields added have terraform variables, we want to
	// make sure FetchDynamicFieldsForJobs knows how to populate those fields
	env.Snap.Snapshot("s3bigstats deploy machinery jobs", &jobSettings)

	databricksJob := databricks.JobSettings{
		Name: "s3bigstats-test-job",
		NewCluster: &databricks.ClusterSpec{
			AwsAttributes: &databricks.ClusterAwsAttributes{},
			CustomTags: map[string]string{
				"samsara:pool:is-production-job": "true",
			},
		},
	}
	env.DatabricksClient.EXPECT().ListInstancePools(ctx).Return(&databricks.ListInstancePoolsOutput{
		InstancePools: []databricks.InstancePoolAndStats{
			{
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "s3bigstatsmerge-production-worker",
				},
				InstancePoolId: "1",
			},
			{
				InstancePoolSpec: databricks.InstancePoolSpec{
					InstancePoolName: "s3bigstatsmerge-production-driver",
				},
				InstancePoolId: "2",
			},
		},
	}, nil).Times(1)

	jobSetting, err := s3bigstats.FetchDynamicFieldsForJobs(ctx, databricksJob, env.DatabricksClient, "us")
	assert.NoError(t, err)
	env.Snap.Snapshot("s3bigstats job settings after updating tf resources", &jobSetting)
}
