package databricksdeploymachineryregistry

import (
	"context"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
)

type testJobDeployMachineryPipeline struct {
	Type string
}

func NewTestJobDeployMachineryPipeline() *testJobDeployMachineryPipeline {
	return &testJobDeployMachineryPipeline{
		Type: "test",
	}
}

func (s *testJobDeployMachineryPipeline) TypeName() string {
	return s.Type
}

func (s *testJobDeployMachineryPipeline) AddJobsToDeploMachineryRegistry(config dataplatformconfig.DatabricksConfig) ([]*dataplatformresource.DatabricksJob, error) {
	return dataplatformresource.DatabricksTestJobSpecs(config)
}

func (s *testJobDeployMachineryPipeline) FetchDynamicFieldsForJobs(ctx context.Context, jobSettings databricks.JobSettings, databricksClient databricks.API, awsRegion string) (databricks.JobSettings, error) {
	return jobSettings, nil
}
