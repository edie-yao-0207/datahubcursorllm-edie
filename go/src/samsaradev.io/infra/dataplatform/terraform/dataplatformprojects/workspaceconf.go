package dataplatformprojects

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/databricks"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
	"samsaradev.io/team"
)

func WorkspaceConfigurationProject(providerGroup string) (*project.Project, error) {
	p := project.Project{
		RootTeam: team.DataPlatform.Name(),
		Provider: providerGroup,
		Class:    "workspace_conf",
	}

	ipAllowList, err := IPAllowList(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "")
	}

	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups,
		ipAllowList,
		WorkspaceConf(),
		map[string][]tf.Resource{
			"aws_provider":        resource.ProjectAWSProvider(&p),
			"tf_backend":          resource.ProjectTerraformBackend(&p),
			"databricks_provider": dataplatformresource.DatabricksOauthProvider(config.Hostname),
		},
	)

	return &p, nil
}

func WorkspaceConf() map[string][]tf.Resource {
	workspaceConfResource := databricksresource.WorkspaceConf{
		CustomConfig: map[databricks.Conf]bool{
			databricks.EnableIPAccessListsConf: true,
		},
	}

	return map[string][]tf.Resource{
		"workspace_conf": {&workspaceConfResource},
	}
}
