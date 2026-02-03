package govramp

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/app/generate_terraform/tf"
	"samsaradev.io/infra/dataplatform/govramp"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

func TestCreateGovrampGrants(t *testing.T) {
	schemaResourceMap := map[string]*databricksresource_official.DatabricksSchema{
		"clouddb": {
			ResourceName: "clouddb",
		},
	}
	viewResourceMap := map[string]*databricksresource_official.SqlTable{
		"clouddb-addresses": {
			ResourceName: "clouddb-addresses",
		},
	}
	nonGovrampTables := []govramp.Table{
		{
			SourceCatalog: "default",
			SourceSchema:  "clouddb",
			SourceTable:   "addresses",
			TableType:     govramp.SourceTableTypeTable,
			TableCategory: govramp.SourceTableCategoryRds,
			CanReadGroups: []components.TeamInfo{
				team.DataPlatform,
				team.DataTools,
			},
			CanReadBiztechGroups: []string{
				"bt-revops-all-users",
			},
		},
	}

	grantsResourceGroups, err := createGovrampGrants(schemaResourceMap, viewResourceMap, nonGovrampTables)
	assert.NoError(t, err)
	assert.Contains(t, grantsResourceGroups, "grants")

	grants := grantsResourceGroups["grants"]

	// Should have 2 total grants: 1 schema + 1 table
	assert.Len(t, grants, 2)

	expectedTeams := []string{"data-platform-group", "data-tools-group", "bt-revops-all-users"}

	// Verify schema grant
	schemaGrant := findGrantByResourceName(grants, "use_schema_non_govramp_customer_data_clouddb")
	assert.NotNil(t, schemaGrant)
	assertGrantHasTeams(t, schemaGrant, expectedTeams, "USE_SCHEMA")

	// Verify table select grant
	tableGrant := findGrantByResourceName(grants, "select_non_govramp_customer_data_clouddb-addresses")
	assert.NotNil(t, tableGrant)
	assertGrantHasTeams(t, tableGrant, expectedTeams, "SELECT")
}

func TestCreateGovrampGrants_ShardedTable(t *testing.T) {

	schemaResourceMap := map[string]*databricksresource_official.DatabricksSchema{
		"safety_shard_0db": {
			ResourceName: "safety_shard_0db",
		},
		"safety_shard_1db": {
			ResourceName: "safety_shard_1db",
		},
		"safety_shard_2db": {
			ResourceName: "safety_shard_2db",
		},
		"safety_shard_3db": {
			ResourceName: "safety_shard_3db",
		},
		"safety_shard_4db": {
			ResourceName: "safety_shard_4db",
		},
		"safety_shard_5db": {
			ResourceName: "safety_shard_5db",
		},
		"safetydb_shards": {
			ResourceName: "safetydb_shards",
		},
	}
	viewResourceMap := map[string]*databricksresource_official.SqlTable{
		"safety_shard_0db-safety_events": {
			ResourceName: "safety_shard_0db-safety_events",
		},
		"safety_shard_1db-safety_events": {
			ResourceName: "safety_shard_1db-safety_events",
		},
		"safety_shard_2db-safety_events": {
			ResourceName: "safety_shard_2db-safety_events",
		},
		"safety_shard_3db-safety_events": {
			ResourceName: "safety_shard_3db-safety_events",
		},
		"safety_shard_4db-safety_events": {
			ResourceName: "safety_shard_4db-safety_events",
		},
		"safety_shard_5db-safety_events": {
			ResourceName: "safety_shard_5db-safety_events",
		},
		"safetydb_shards-safety_events": {
			ResourceName: "safetydb_shards-safety_events",
		},
	}
	nonGovrampTables := []govramp.Table{
		{
			SourceCatalog: "default",
			SourceSchema:  "safetydb_shards",
			SourceTable:   "safety_events",
			TableType:     govramp.SourceTableTypeView,
			TableCategory: govramp.SourceTableCategoryRdsSharded,
			CanReadGroups: []components.TeamInfo{
				team.DataEngineering,
				team.DataTools,
			},
		},
	}

	grantsResourceGroups, err := createGovrampGrants(schemaResourceMap, viewResourceMap, nonGovrampTables)
	assert.NoError(t, err)
	assert.Contains(t, grantsResourceGroups, "grants")

	grants := grantsResourceGroups["grants"]

	// Should have 14 total grants: 7 schemas (main + 6 shards) + 7 tables (main + 6 shards)
	assert.Len(t, grants, 14)

	// Verify main schema grant
	mainSchemaGrant := findGrantByResourceName(grants, "use_schema_non_govramp_customer_data_safetydb_shards")
	assert.NotNil(t, mainSchemaGrant)
	assertGrantHasTeams(t, mainSchemaGrant, []string{"data-engineering-group", "data-tools-group"}, "USE_SCHEMA")

	// Verify shard schema grants
	for i := 0; i < 6; i++ {
		shardSchemaName := fmt.Sprintf("use_schema_non_govramp_customer_data_safety_shard_%ddb", i)
		shardGrant := findGrantByResourceName(grants, shardSchemaName)
		assert.NotNil(t, shardGrant, "Missing grant for %s", shardSchemaName)
		assertGrantHasTeams(t, shardGrant, []string{"data-engineering-group", "data-tools-group"}, "USE_SCHEMA")
	}

	// Verify main table select grant
	mainTableGrant := findGrantByResourceName(grants, "select_non_govramp_customer_data_safetydb_shards-safety_events")
	assert.NotNil(t, mainTableGrant)
	assertGrantHasTeams(t, mainTableGrant, []string{"data-engineering-group", "data-tools-group"}, "SELECT")

	// Verify shard table select grants
	for i := 0; i < 6; i++ {
		shardTableName := fmt.Sprintf("select_non_govramp_customer_data_safety_shard_%ddb-safety_events", i)
		shardGrant := findGrantByResourceName(grants, shardTableName)
		assert.NotNil(t, shardGrant, "Missing grant for %s", shardTableName)
		assertGrantHasTeams(t, shardGrant, []string{"data-engineering-group", "data-tools-group"}, "SELECT")
	}
}

// Helper function to find a grant by resource name
func findGrantByResourceName(grants []tf.Resource, resourceName string) *databricksresource_official.DatabricksGrants {
	for _, grant := range grants {
		if dbGrant, ok := grant.(*databricksresource_official.DatabricksGrants); ok {
			if dbGrant.ResourceName == resourceName {
				return dbGrant
			}
		}
	}
	return nil
}

// Helper function to assert that a grant has the expected teams and privilege
func assertGrantHasTeams(t *testing.T, grant *databricksresource_official.DatabricksGrants, expectedTeams []string, expectedPrivilege string) {
	assert.Len(t, grant.Grants, len(expectedTeams))

	actualTeams := make([]string, len(grant.Grants))
	for i, grantSpec := range grant.Grants {
		actualTeams[i] = grantSpec.Principal
		assert.Len(t, grantSpec.Privileges, 1)
		assert.Equal(t, expectedPrivilege, string(grantSpec.Privileges[0]))
	}

	// Sort both slices to ensure order-independent comparison
	sort.Strings(expectedTeams)
	sort.Strings(actualTeams)
	assert.Equal(t, expectedTeams, actualTeams)
}

// TestCanReadBiztechGroups_Validation validates that all CanReadBiztechGroups entries:
// 1. Follow the 'bt-' naming convention for external biztech groups
// 2. Do not conflict with R&D team groups (those should use CanReadGroups instead)
func TestCanReadBiztechGroups_Validation(t *testing.T) {
	allTables := govramp.GetAllNonGovrampTables()

	// Get all official team groups to ensure biztech groups don't overlap
	hierarchy, err := dataplatformprojects.MakeGroupHierarchy()
	require.NoError(t, err, "Failed to get group hierarchy")

	officialGroups := make(map[string]struct{})
	for _, group := range hierarchy.AllGroups {
		officialGroups[group.DatabricksAccountGroupName()] = struct{}{}
	}

	for _, table := range allTables {
		tableKey := fmt.Sprintf("%s.%s", table.SourceSchema, table.SourceTable)

		for _, groupName := range table.CanReadBiztechGroups {
			// Validation 1: Must follow 'bt-' naming convention
			assert.True(t,
				databaseregistries.IsValidBiztechGroupName(groupName),
				"Table %s has CanReadBiztechGroups entry '%s' that is not valid (e.g. does not start with 'bt-')",
				tableKey, groupName)

			// Validation 2: Must not conflict with R&D team groups
			_, isOfficialGroup := officialGroups[groupName]
			assert.False(t,
				isOfficialGroup,
				"Table %s has CanReadBiztechGroups entry '%s' that conflicts with an R&D team group (should use CanReadGroups instead)",
				tableKey, groupName)
		}
	}
}
