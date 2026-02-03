package govramp

import (
	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/govramp"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource/databricksofficialprovider"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/resource"
)

const (
	govrampCatalog = "non_govramp_customer_data"
)

func GovrampProjects(providerGroup string) ([]*project.Project, error) {
	config, err := dataplatformconfig.DatabricksProviderConfig(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "retrieving provider config")
	}

	// Create the combined project
	combinedProject, err := govrampCombinedProject(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "building combined govramp project")
	}

	// Add common resource groups (terraform backend and provider)
	combinedProject.ResourceGroups = project.MergeResourceGroups(combinedProject.ResourceGroups, map[string][]tf.Resource{
		"tf_backend":          resource.ProjectTerraformBackend(combinedProject),
		"databricks_provider": databricksofficialprovider.DatabricksOauthWorkspaceProvider(config.Hostname),
	})

	return []*project.Project{combinedProject}, nil
}

// govrampCombinedProject creates a single project containing both schemas and views
func govrampCombinedProject(providerGroup string) (*project.Project, error) {
	// First, create the schema resources (these must be created before views)
	schemaResourceGroups, schemaResourceMap, err := createGovrampSchemaResources(providerGroup)
	if err != nil {
		return nil, oops.Wrapf(err, "creating schema resources")
	}

	// Then, create the view resources that depend on the schemas
	viewResourceGroups, viewResourceMap, err := createGovrampViewResources(schemaResourceMap)
	if err != nil {
		return nil, oops.Wrapf(err, "creating view resources")
	}

	grantsResourceGroups, err := createGovrampGrants(schemaResourceMap, viewResourceMap, govramp.GetAllNonGovrampTables())
	if err != nil {
		return nil, oops.Wrapf(err, "creating grants resources")
	}

	// Combine both resource groups into a single project
	combinedResourceGroups := project.MergeResourceGroups(schemaResourceGroups, viewResourceGroups, grantsResourceGroups)

	combinedProject := &project.Project{
		RootTeam:        dataplatformterraformconsts.DataPlatformNonGovrampCustomerDataTerraformProjectPipeline,
		Provider:        providerGroup,
		Class:           "govramp",
		Name:            "views",
		ResourceGroups:  combinedResourceGroups,
		GenerateOutputs: true, // Enable outputs in case other projects need to reference this
	}

	return combinedProject, nil
}
