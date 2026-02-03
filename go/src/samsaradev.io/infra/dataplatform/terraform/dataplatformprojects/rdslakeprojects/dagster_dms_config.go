package rdslakeprojects

import (
	"fmt"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
)

// Generate shared initial load functionality
func dagsterDmsConfigProject(config dataplatformconfig.DatabricksConfig) (*project.Project, error) {
	dagsterFiles, err := dagsterConfigFiles(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "building dagster files")
	}

	p := &project.Project{
		RootTeam: dataplatformterraformconsts.DataLakeRdsIngestionTerraformProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "dagster-dms",
		ResourceGroups: project.MergeResourceGroups(
			dagsterFiles,
		),
		GenerateOutputs: true,
	}
	return p, nil
}

func dagsterConfigFiles(region string) (map[string][]tf.Resource, error) {
	dagsterConfig, err := dataplatformresource.DeployedArtifactObjectNoHash(region, fmt.Sprintf("go/src/samsaradev.io/infra/dataplatform/rdsdeltalake/dagsterconfig/generated/%s/dagster_config.json", region))
	if err != nil {
		return nil, oops.Wrapf(err, "make s3 object for dagster_config")
	}

	// Also upload the Kinesis-specific Dagster config
	kinesisDagsterConfig, err := dataplatformresource.DeployedArtifactObjectNoHash(region, fmt.Sprintf("go/src/samsaradev.io/infra/dataplatform/rdsdeltalake/dagsterconfig/generated/%s/kinesis_dms_dagster_config.json", region))
	if err != nil {
		return nil, oops.Wrapf(err, "make s3 object for kinesis_dms_dagster_config")
	}

	return map[string][]tf.Resource{
		"config": {dagsterConfig, kinesisDagsterConfig},
	}, nil
}
