package datastreamlakeprojects

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
)

func AllProjects(providerGroup string) ([]*project.Project, error) {
	var projects []*project.Project
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	glueProjects, err := glueProjects(config)
	if err != nil {
		return nil, oops.Wrapf(err, "Unable to generate glue resources for data streams")
	}
	projects = append(projects, glueProjects...)

	firehoseProjects, err := firehoseProjects(config)
	if err != nil {
		return nil, oops.Wrapf(err, "Unable to generate firehose projects for data streams")
	}
	projects = append(projects, firehoseProjects...)

	for _, p := range projects {
		p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
			"aws_provider": resource.ProjectAWSProvider(p),
			"tf_backend":   resource.ProjectTerraformBackend(p),
		})
	}

	return projects, nil
}
