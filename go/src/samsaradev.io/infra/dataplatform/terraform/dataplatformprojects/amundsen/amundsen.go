package amundsen

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/awsresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/team"
)

// https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html
func AmundsenProject(config dataplatformconfig.DatabricksConfig) (*project.Project, error) {

	s3ResourceGroups := s3Resources(config)

	dbxResourceGroups, err := dbxResources(config)
	if err != nil {
		return nil, oops.Wrapf(err, "error generating databricks resources for amundsen")
	}

	p := &project.Project{
		RootTeam:        team.DataPlatform.Name(),
		Provider:        config.DatabricksProviderGroup,
		GenerateOutputs: true,
		Class:           "amundsen",
		ResourceGroups: project.MergeResourceGroups(
			s3ResourceGroups,
			dbxResourceGroups,
		),
	}

	awsProviderVersion := "5.25.0"
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
		map[string][]tf.Resource{
			"aws_provider":        resource.ProjectAWSProvider(p, resource.WithAWSProviderVersion(awsProviderVersion)),
			"tf_backend":          resource.ProjectTerraformBackend(p, resource.WithRequiredAWSProviderVersionAndSource(awsProviderVersion, resource.TerraformRegistryAWSProviderSource)),
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		})

	return p, nil
}

// getAmundsenTags generates a slice of tags for an Amundsen resource based on the service name
func getAmundsenTags(serviceName string) []*awsresource.Tag {
	return []*awsresource.Tag{
		{
			Key:   "samsara:team",
			Value: strings.ToLower(team.DataPlatform.TeamName),
		},
		{
			Key:   "samsara:product-group",
			Value: strings.ToLower(team.TeamProductGroup[team.DataPlatform.TeamName]),
		},
		{
			Key:   "samsara:service",
			Value: fmt.Sprintf("amundsen-%s", serviceName),
		},
		{
			Key:   "samsara:rnd-allocation",
			Value: "1",
		},
	}
}
