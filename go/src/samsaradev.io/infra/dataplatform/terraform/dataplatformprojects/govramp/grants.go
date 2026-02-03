package govramp

import (
	"fmt"
	"slices"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/govramp"
)

func elementExists(slice []string, element string) bool {
	for _, e := range slice {
		if e == element {
			return true
		}
	}
	return false
}

// createGovrampGrants creates grants for each team to use the catalog, schema, and select view.
// grants are created for teams that have CanReadGroups (components.TeamInfo) and CanReadBiztechGroups (string group names) set in the non_govramp_registry.go file.
// We also assume that nonRnD teams have USE CATALOG and USE SCHEMA privileges on the default schema for now, we can change this later.
func createGovrampGrants(schemaResourceMap map[string]*databricksresource_official.DatabricksSchema, viewResourceMap map[string]*databricksresource_official.SqlTable, nonGovrampTables []govramp.Table) (map[string][]tf.Resource, error) {
	resourceGroups := make(map[string][]tf.Resource)
	uniqueTeamUseSchema := make(map[string][]string)
	uniqueTeamSelectGrants := make(map[string][]string)
	uniqueTeamUseCatalog := []string{}
	for _, table := range nonGovrampTables {
		if table.GrantAccessToAllGovrampRegistryTeams {
			allUniqueGroupNames := getAllUniqueGroupNamesFromRegistry()
			for _, groupName := range allUniqueGroupNames {
				uniqueTeamUseSchema[table.SourceSchema] = append(uniqueTeamUseSchema[table.SourceSchema], groupName)
				uniqueTeamSelectGrants[fmt.Sprintf("%s-%s", table.SourceSchema, table.SourceTable)] = append(uniqueTeamSelectGrants[fmt.Sprintf("%s-%s", table.SourceSchema, table.SourceTable)], groupName)
			}
			continue
		}

		shardNames := []string{}
		var err error

		// Handle for Sharded RDS Tables
		// Fetch the shard names from the schema name.
		if table.TableCategory == govramp.SourceTableCategoryRdsSharded {
			rdsDbName := strings.TrimSuffix(table.SourceSchema, "db_shards")
			shardNames, err = getShardNames(rdsDbName)
			if err != nil {
				return nil, oops.Wrapf(err, "failed to get shard names for %s", table.SourceSchema)
			}
		}

		// Handle CanReadGroups (components.TeamInfo)
		for _, team := range table.CanReadGroups {
			if !elementExists(uniqueTeamUseSchema[table.SourceSchema], team.DatabricksAccountGroupName()) {
				uniqueTeamUseSchema[table.SourceSchema] = append(uniqueTeamUseSchema[table.SourceSchema], team.DatabricksAccountGroupName())
			}

			name := fmt.Sprintf("%s-%s", table.SourceSchema, table.SourceTable)
			uniqueTeamSelectGrants[name] = append(uniqueTeamSelectGrants[name], team.DatabricksAccountGroupName())

			if !elementExists(uniqueTeamUseCatalog, team.DatabricksAccountGroupName()) {
				uniqueTeamUseCatalog = append(uniqueTeamUseCatalog, team.DatabricksAccountGroupName())
			}

			// We need to grant on all shards of the schema and the table if the table is sharded.
			for _, shardName := range shardNames {
				shardSchemaName := fmt.Sprintf("%sdb", shardName)
				if !elementExists(uniqueTeamUseSchema[shardSchemaName], team.DatabricksAccountGroupName()) {
					uniqueTeamUseSchema[shardSchemaName] = append(uniqueTeamUseSchema[shardSchemaName], team.DatabricksAccountGroupName())
				}
				shardName = fmt.Sprintf("%s-%s", shardSchemaName, table.SourceTable)
				uniqueTeamSelectGrants[shardName] = append(uniqueTeamSelectGrants[shardName], team.DatabricksAccountGroupName())
			}
		}

		// Handle CanReadBiztechGroups (string group names)
		for _, groupName := range table.CanReadBiztechGroups {
			if !elementExists(uniqueTeamUseSchema[table.SourceSchema], groupName) {
				uniqueTeamUseSchema[table.SourceSchema] = append(uniqueTeamUseSchema[table.SourceSchema], groupName)
			}

			name := fmt.Sprintf("%s-%s", table.SourceSchema, table.SourceTable)
			uniqueTeamSelectGrants[name] = append(uniqueTeamSelectGrants[name], groupName)

			if !elementExists(uniqueTeamUseCatalog, groupName) {
				uniqueTeamUseCatalog = append(uniqueTeamUseCatalog, groupName)
			}

			// We need to grant on all shards of the schema and the table if the table is sharded.
			for _, shardName := range shardNames {
				shardSchemaName := fmt.Sprintf("%sdb", shardName)
				if !elementExists(uniqueTeamUseSchema[shardSchemaName], groupName) {
					uniqueTeamUseSchema[shardSchemaName] = append(uniqueTeamUseSchema[shardSchemaName], groupName)
				}
				shardName = fmt.Sprintf("%s-%s", shardSchemaName, table.SourceTable)
				uniqueTeamSelectGrants[shardName] = append(uniqueTeamSelectGrants[shardName], groupName)
			}
		}
	}

	// Create use Schema grants for each team.
	for schema, teams := range uniqueTeamUseSchema {
		schemaLevelGrants := []databricksresource_official.GrantSpec{}
		for _, team := range teams {
			schemaLevelGrants = append(schemaLevelGrants, databricksresource_official.GrantSpec{
				Principal: team,
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantSchemaUseSchema,
				},
			})
		}
		resourceGroups["grants"] = append(resourceGroups["grants"], &databricksresource_official.DatabricksGrants{
			ResourceName: fmt.Sprintf("use_schema_%s_%s", govrampCatalog, schema),
			Schema:       schemaResourceMap[schema].ResourceId().ReferenceAttr("id"),
			Grants:       schemaLevelGrants,
		})
	}

	// Create select grants for each team.
	for tableName, teams := range uniqueTeamSelectGrants {
		selectGrants := []databricksresource_official.GrantSpec{}
		for _, team := range teams {
			selectGrants = append(selectGrants, databricksresource_official.GrantSpec{
				Principal: team,
				Privileges: []databricksresource_official.GrantPrivilege{
					databricksresource_official.GrantViewSelect,
				},
			})

		}
		resourceGroups["grants"] = append(resourceGroups["grants"], &databricksresource_official.DatabricksGrants{
			ResourceName: fmt.Sprintf("select_%s_%s", govrampCatalog, tableName),
			Table:        viewResourceMap[tableName].ResourceId().ReferenceAttr("id"),
			Grants:       selectGrants,
		})
	}

	return resourceGroups, nil
}

// Function to iterate through the entire govramp registry and fetch the unique list of
// teams in the CanReadGroups and CanReadBiztechGroups fields.
func getAllUniqueGroupNamesFromRegistry() []string {
	groupNames := []string{}
	for _, table := range govramp.GetAllNonGovrampTables() {
		for _, team := range table.CanReadGroups {
			groupNames = append(groupNames, team.DatabricksAccountGroupName())
		}
		for _, groupName := range table.CanReadBiztechGroups {
			groupNames = append(groupNames, groupName)
		}
	}
	slices.Sort(groupNames)
	return slices.Compact(groupNames)
}
