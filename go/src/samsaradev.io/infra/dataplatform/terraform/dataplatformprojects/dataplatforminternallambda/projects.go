package dataplatforminternallambda

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
)

func AllProjects(providerGroup string) ([]*project.Project, error) {
	_, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}
	var projects []*project.Project

	databricksProject, err := dataplatformInternalTestProjects(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "dataplatforminternaltest project")
	}
	projects = append(projects, databricksProject)

	for _, p := range projects {
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(p),
			"tf_backend":   resource.ProjectTerraformBackend(p),
		})
	}
	return projects, nil
}
