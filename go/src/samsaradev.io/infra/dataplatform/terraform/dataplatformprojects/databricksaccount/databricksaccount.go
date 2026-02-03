package databricksaccount

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource/databricksofficialprovider"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
)

func DatabricksAccountProject(providerGroup string) (*project.Project, error) {
	p := &project.Project{
		RootTeam: dataplatformterraformconsts.DataPlatformOfficialDatabricksProviderTerraformProjectPipeline,
		Provider: providerGroup,
		Class:    "databricksaccount",
		ResourceGroups: map[string][]tf.Resource{
			"databricks_provider": databricksofficialprovider.DatabricksAccountProvider(),
		},
	}
	accountGroups, err := AccountGroups()
	if err != nil {
		return nil, oops.Wrapf(err, "creating account level group resources")
	}
	workspaces, err := MultiWorkSpacesProject()
	if err != nil {
		return nil, oops.Wrapf(err, "creating workspaces in databricks accounts")
	}
	metastores, err := unityCatalogMetastoreResources()
	if err != nil {
		return nil, oops.Wrapf(err, "creating unity catalog metastore resources")
	}
	databricksBudgetPolicy, err := DatabricksBudgetPolicyResources()
	if err != nil {
		return nil, oops.Wrapf(err, "creating databricks budget policy resources")
	}

	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, accountGroups, workspaces, metastores, databricksBudgetPolicy)
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"tf_backend": resource.ProjectTerraformBackend(p),
	})

	return p, nil
}
