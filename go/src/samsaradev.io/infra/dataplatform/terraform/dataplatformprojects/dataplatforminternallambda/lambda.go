package dataplatforminternallambda

import (
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/lambdas"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/libs/ni/pointer"
	"samsaradev.io/team"
)

func dataplatformInternalTestProjects(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}
	resourceGroups := make(map[string][]tf.Resource)

	lambdaFunctions, err := dataplatformInternalTestLambdas(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	lambdaFiles := make(map[string]*project.ExtraFile)
	for _, lambdaFunction := range lambdaFunctions {
		functionFiles, functionResources, err := lambdaFunction.FilesAndResources(dataplatformresource.ArtifactBucket(config.Region))
		if err != nil {
			return nil, oops.Wrapf(err, "")
		}
		resourceGroups["lambda_"+lambdaFunction.LambdaFunctionName()] = functionResources

		for k, v := range functionFiles {
			lambdaFiles[k] = v
		}
	}

	return &project.Project{
		RootTeam:       team.DataPlatform.Name(),
		Provider:       providerGroup,
		Class:          "dataplatform-internal-test-lambdas",
		Name:           "test-lambdas",
		ResourceGroups: resourceGroups,
		ExtraFiles:     lambdaFiles,
	}, nil
}

func dataplatformInternalTestLambdas(region string) ([]lambdas.LambdaFunction, error) {
	var lambdaFunctions []lambdas.LambdaFunction

	dataplatformInternalTestLambda := &lambdas.GoLambdaFunction{
		Name:           "dataplatform_internal_test_lambda",
		MainFile:       "go/src/samsaradev.io/infra/dataplatform/lambdafunctions/dataplatforminternal/main.go",
		Region:         region,
		TimeoutSeconds: 5 * 60,
		MemorySizeInMB: 1024,
		Publish:        pointer.BoolPtr(true),
		Tags: map[string]string{
			"samsara:service":       "dataplatform_internal_test_lambda",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	lambdaFunctions = append(lambdaFunctions, dataplatformInternalTestLambda)

	return lambdaFunctions, nil

}
