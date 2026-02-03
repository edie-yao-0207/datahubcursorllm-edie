package emrreplicationproject

import (
	"fmt"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/genericresource"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/emrreplication"
	"samsaradev.io/infra/dataplatform/emrreplication/emrhelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/dataplatformterraformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformresource/databricksofficialprovider"
	"samsaradev.io/infra/terraform/cmd/generate_terraform_team/project"
)

func emrReplicationCustomViews(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	views := []tf.Resource{}
	for _, entity := range emrreplication.GetAllEmrReplicationSpecs() {
		if entity.ViewDefinition == nil {
			continue
		}

		cells, err := emrhelpers.GetCellsForEntity(entity, config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "getting cells for entity %s", entity.Name)
		}
		ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "no CI service principal app id for region %s", config.Region)
		}
		for _, cell := range cells {
			tableName := fmt.Sprintf("%s.%s", emrReplicationDatabase(cell), strings.ToLower(entity.Name))
			sqlString := fmt.Sprintf(entity.ViewDefinition.SqlString, tableName)

			// Initialize the SqlTable resource with necessary fields
			sqlTable := &databricksresource_official.SqlTable{
				ResourceName:   fmt.Sprintf("%s_%s", entity.Name, cell),
				Name:           entity.ViewDefinition.ViewName,
				CatalogName:    "default",
				SchemaName:     strings.ToLower(emrReplicationDatabase(cell)),
				TableType:      "VIEW",
				ViewDefinition: sqlString,
				Comment:        fmt.Sprintf("View for %s", entity.Name),
				WarehouseId:    genericresource.DataResourceId("databricks_sql_warehouse", "aianddata").ReferenceAttr("id"),
				Owner:          ciServicePrincipalAppId,
			}
			views = append(views, sqlTable)
		}
	}
	return views, nil
}

func GetCombinedView(tableName string, cells []string, config dataplatformconfig.DatabricksConfig) (*databricksresource_official.SqlTable, error) {
	// Build the SQL query dynamically using the cells
	var unionClauses []string
	for _, cell := range cells {
		unionClause := fmt.Sprintf("\tselect '%s' as cell, * from emr_%s.%s",
			cell,
			cell,
			tableName)
		unionClauses = append(unionClauses, unionClause)
	}

	// skiplint: +banselectstar
	sqlString := "SELECT * FROM (\n" + strings.Join(unionClauses, "\n\tUNION\n") + "\n)"

	// Create the SQL view resource
	ciServicePrincipalAppId, err := dataplatformconfig.GetCIServicePrincipalAppIdByRegion(config.Region)
	if err != nil {
		return nil, oops.Wrapf(err, "no CI service principal app id for region %s", config.Region)
	}

	sqlTable := &databricksresource_official.SqlTable{
		ResourceName:   fmt.Sprintf("combined_%s", tableName),
		Name:           tableName,
		CatalogName:    "default",
		SchemaName:     "emr",
		TableType:      "VIEW",
		ViewDefinition: sqlString,
		Comment:        fmt.Sprintf("Combined view across all cells for %s", tableName),
		WarehouseId:    genericresource.DataResourceId("databricks_sql_warehouse", "aianddata").ReferenceAttr("id"),
		Owner:          ciServicePrincipalAppId,
	}
	return sqlTable, nil
}

func emrReplicationCombinedViews(config dataplatformconfig.DatabricksConfig) ([]tf.Resource, error) {
	views := []tf.Resource{}

	for _, entity := range emrreplication.GetAllEmrReplicationSpecs() {
		cells, err := emrhelpers.GetCellsForEntity(entity, config.Region)
		if err != nil {
			return nil, oops.Wrapf(err, "getting cells for entity %s", entity.Name)
		}

		combinedView, err := GetCombinedView(strings.ToLower(entity.Name), cells, config)
		if err != nil {
			return nil, oops.Wrapf(err, "creating combined view for entity %s", entity.Name)
		}
		views = append(views, combinedView)

		if entity.ViewDefinition != nil {
			combinedCustomView, err := GetCombinedView(entity.ViewDefinition.ViewName, cells, config)
			if err != nil {
				return nil, oops.Wrapf(err, "creating combined view for entity %s custom view %s", entity.Name, entity.ViewDefinition.ViewName)
			}
			views = append(views, combinedCustomView)
		}

	}
	return views, nil
}

func EmrReplicationViewsProject(config dataplatformconfig.DatabricksConfig) (*project.Project, error) {
	// SQL Warehouse to create views
	warehouse := &genericresource.Data{
		Type: "databricks_sql_warehouse",
		Name: "aianddata",
		Args: databricksresource_official.SqlWarehouseArgs{
			Name: "aianddata",
		},
	}

	// Get all custom views as defined in the emr replication registry
	customViews, err := emrReplicationCustomViews(config)
	if err != nil {
		return nil, oops.Wrapf(err, "creating emr replication custom views")
	}

	combinedViews, err := emrReplicationCombinedViews(config)
	if err != nil {
		return nil, oops.Wrapf(err, "creating emr replication combined views")
	}

	return &project.Project{
		RootTeam: dataplatformterraformconsts.DataPlatformOfficialDatabricksProviderTerraformProjectPipeline,
		Provider: config.DatabricksProviderGroup,
		Class:    "emr_replication",
		Name:     "emr_replication_views",
		ResourceGroups: map[string][]tf.Resource{
			"databricks_provider": databricksofficialprovider.DatabricksOauthWorkspaceProvider(config.Hostname),
			"custom_views":        customViews,
			"combined_views":      combinedViews,
			"resources":           {warehouse},
		},
		GenerateOutputs: true,
	}, nil
}
