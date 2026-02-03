package databricksdeploymachineryregistry

import (
	"context"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/service/pipelines"
)

type DeployMachineryDatabricksJobType interface {
	TypeName() string
	AddJobsToDeploMachineryRegistry(config dataplatformconfig.DatabricksConfig) ([]*dataplatformresource.DatabricksJob, error)
	FetchDynamicFieldsForJobs(ctx context.Context, jobSettings databricks.JobSettings, databricksClient databricks.API, awsRegion string) (databricks.JobSettings, error)
}

var DeployMachineryTypeList = []DeployMachineryDatabricksJobType{
	NewS3BigStatsDeployMachineryPipeline(),
	NewTestJobDeployMachineryPipeline(),
}

var DeployMachineryTypeDictionary = map[string]DeployMachineryDatabricksJobType{}

func init() {
	for _, DeployMachineryType := range DeployMachineryTypeList {
		DeployMachineryTypeDictionary[DeployMachineryType.TypeName()] = DeployMachineryType
	}
}

func addJobs(config dataplatformconfig.DatabricksConfig, deployMachineryDatabricksJobType DeployMachineryDatabricksJobType, jobs []*dataplatformresource.DatabricksJob) ([]*dataplatformresource.DatabricksJob, error) {
	deployMachineryJobs, err := deployMachineryDatabricksJobType.AddJobsToDeploMachineryRegistry(config)
	if err != nil {
		return nil, oops.Wrapf(err, "error adding deploy machinery jobs to  pipelines")
	}
	jobs = append(jobs, deployMachineryJobs...)
	return jobs, nil
}

func RegisterJobs(awsRegion string) ([]*dataplatformresource.DatabricksJob, error) {
	jobs := make([]*dataplatformresource.DatabricksJob, 0)

	// All jobs in the eu region should be a part of the stable stage.
	// We don't want any beta jobs in the EU or Canada
	for _, job := range jobs {
		switch job.DatabricksJobSpec.Region {
		case infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion:
			job.RolloutStage = pipelines.DatabricksJobProdRolloutState
		}
	}
	return jobs, nil
}
