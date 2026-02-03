package rdslakeprojects

import (
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/lambdas"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/team"
)

func dmsLambdaProject(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}
	resourceGroups := make(map[string][]tf.Resource)

	lambdaFunctions, err := dmsPasswordManagerProject(config.Region)
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
		Provider:       config.AWSProviderGroup,
		Class:          "dataplatform-rds-lambdas",
		Name:           "rds-lambdas",
		ResourceGroups: resourceGroups,
		ExtraFiles:     lambdaFiles,
	}, nil
}

func dmsPasswordManagerProject(region string) ([]lambdas.LambdaFunction, error) {
	var lambdaFunctions []lambdas.LambdaFunction

	databricksPollerLambda := &lambdas.PythonLambdaFunction{
		Name:           "dms_password_manager",
		SourceBasePath: "python3/samsaradev/infra/dataplatform/lambda/dms",
		Region:         region,
		Handler:        "password_manager.lambda_handler",
		Runtime:        awsresource.LambdaFunctionRuntimePython310,
		RequirementsFiles: []string{
			"python3/requirements.lambda.txt",
		},
		TimeoutSeconds: 120,
		Tags: map[string]string{
			"samsara:service":       "dms_password_manager",
			"samsara:team":          strings.ToLower(team.DataPlatform.TeamName),
			"samsara:product-group": strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
	}
	lambdaFunctions = append(lambdaFunctions, databricksPollerLambda)

	return lambdaFunctions, nil

}
