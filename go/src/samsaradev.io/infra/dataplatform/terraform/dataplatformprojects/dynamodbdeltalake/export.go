package dynamodbdeltalake

import (
	"slices"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/dynamodbdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
)

func dynamodbDeltaLakeExportProject(providerGroup string) (*project.Project, error) {

	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	resourceGroups := make(map[string][]tf.Resource)

	for _, table := range dynamodbdeltalake.AllTables() {

		if slices.Contains(table.InternalOverrides.UnavailableRegions, config.Region) {
			// if the dynamo table is not available in this region, skip it
			continue
		}

		export := CreateLoadResource(config, table)
		if export != nil {
			resourceGroups["load_resources"] = append(resourceGroups["load_resources"], export)
		}
	}

	return &project.Project{
		RootTeam:        dataplatformterraformconsts.DataPlatformDynamodbDeltaLakeProjectPipeline,
		Provider:        config.AWSProviderGroup,
		Class:           DynamodbDeltaLakeResourceBaseName,
		Name:            DynamodbDeltaLakeResourceBaseName + "-export",
		GenerateOutputs: true,
		ResourceGroups:  resourceGroups,
	}, nil

}
