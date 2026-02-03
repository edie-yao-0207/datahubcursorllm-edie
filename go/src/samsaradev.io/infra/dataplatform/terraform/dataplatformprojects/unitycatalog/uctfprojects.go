package unitycatalog

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource/databricksofficialprovider"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
)

func UnityCatalogProjects(providerGroup string) ([]*project.Project, error) {
	infra, err := infraProject(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "creating infra project")
	}
	data, err := dataProject(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "creating data project")
	}
	return []*project.Project{infra, data}, nil
}

// Project for infra-y resources within databricks. This is the metastore object,
// metastore <> workplace associations, etc.
func infraProject(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// Create a project in the official tf provider buildkite for the unity catalog
	// metastore resources.
	p := &project.Project{
		RootTeam: dataplatformterraformconsts.DataPlatformOfficialDatabricksProviderTerraformProjectPipeline,
		Provider: providerGroup,
		Class:    "unitycatalog",
		ResourceGroups: map[string][]tf.Resource{
			"databricks_provider": databricksofficialprovider.DatabricksOauthWorkspaceProvider(config.Hostname),
		},
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"tf_backend":   resource.ProjectTerraformBackend(p),
		"aws_provider": resource.ProjectAWSProvider(p),
	})

	serviceCredentialResources, err := serviceCredentialResources(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "creating official provider cloud credential project")
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, serviceCredentialResources)

	return p, nil
}

// Project for data resources like catalogs, tables, etc.
func dataProject(providerGroup string) (*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	p := &project.Project{
		RootTeam: dataplatformterraformconsts.DataPlatformOfficialDatabricksProviderTerraformProjectPipeline,
		Provider: providerGroup,
		Class:    "unitycatalog-data",
		ResourceGroups: map[string][]tf.Resource{
			"databricks_provider": databricksofficialprovider.DatabricksOauthWorkspaceProvider(config.Hostname),
		},
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, map[string][]tf.Resource{
		"tf_backend":   resource.ProjectTerraformBackend(p),
		"aws_provider": resource.ProjectAWSProvider(p),
	})

	shareResources, err := unityCatalogShareResources(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "creating Delta Shares")
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, shareResources)

	recipientResources, err := unityCatalogRecipientResources(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "creating Delta Share Recipients")
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, recipientResources)

	shareCatalogResources, err := unityCatalogShareCatalogResources(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "creating Delta Share Catalog")
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, shareCatalogResources)

	storageResources, err := unityCatalogExternalLocations(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "creating storage credentials")
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, storageResources)

	catalogResources, err := unityCatalogCatalogResources(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "creating unity catalog catalog resources")
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, catalogResources)

	allowlistResources, err := unityCatalogArtifactAllowlist(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "creating unity catalog artifact allowlist")
	}
	p.ResourceGroups = project.MergeResourceGroups(p.ResourceGroups, allowlistResources)

	return p, nil
}
