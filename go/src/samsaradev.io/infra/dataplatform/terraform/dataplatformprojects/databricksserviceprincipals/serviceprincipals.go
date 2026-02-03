package databricksserviceprincipals

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource/databricksofficialprovider"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
)

func DatabricksServicePrincipalsProject(providerGroup string, prodWorkspace bool) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}
	p := &project.Project{
		RootTeam: dataplatformterraformconsts.DataPlatformOfficialDatabricksProviderTerraformProjectPipeline,
		Provider: providerGroup,
		Class:    "databricksserviceprincipals",
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"tf_backend":          resource.ProjectTerraformBackend(p),
		"databricks_provider": databricksofficialprovider.DatabricksOauthWorkspaceProvider(config.Hostname),
	})
	resources, err := createServicePrincipalResources(config.Region, prodWorkspace)
	if err != nil {
		return nil, oops.Wrapf(err, "creating resources")
	}
	p.ResourceGroups["service_principals"] = resources
	return p, nil
}

// prodWorkspace is a flag to indicate if the service principal is used in the prod workspace.
// If true, the service principal will be added to the prod workspace
// If false, the service principal will not be added to the dev workspace.
// As a reminder, the prod workspace is the test workspace for databricks.
func createServicePrincipalResources(region string, prodWorkspace bool) ([]tf.Resource, error) {
	resources := []tf.Resource{}

	for _, sp := range ServicePrincipals {

		// If we are in prod workspace and the service principal is not used in prod, skip it.
		if prodWorkspace && !sp.ProdWorkspace {
			continue
		}

		// If we are in dev workspace and the service principal is used in prod, skip it.
		if !prodWorkspace && sp.ProdWorkspace {
			continue
		}

		regionalName, err := sp.RegionalServicePrincipalName(region)
		if err != nil {
			return nil, oops.Wrapf(err, "getting regional service principal name")
		}

		servicePrincipalResource := &databricksresource_official.DatabricksServicePrincipal{
			DisplayName:             regionalName,
			AllowClusterCreate:      sp.AllowClusterCreate,
			AllowInstancePoolCreate: sp.AllowInstancePoolCreate,
			DatabricksSqlAccess:     sp.DatabricksSqlAccess,
			WorkspaceAccess:         sp.WorkspaceAccess,
			Active:                  sp.Active,
		}

		resources = append(resources, servicePrincipalResource)

	}

	return resources, nil
}
