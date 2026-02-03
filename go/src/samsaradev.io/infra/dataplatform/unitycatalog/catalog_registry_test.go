package unitycatalog

import (
	"fmt"
	"sort"
	"testing"

	"github.com/samsarahq/go/snapshotter"

	"samsaradev.io/infra/dataplatform/govramp"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
)

func uniqueSorted(items []string) []string {
	m := make(map[string]struct{}, len(items))
	for _, it := range items {
		m[it] = struct{}{}
	}
	out := make([]string, 0, len(m))
	for it := range m {
		out = append(out, it)
	}
	sort.Strings(out)
	return out
}

func TestCatalogConfiguration(t *testing.T) {
	var errors []string

	config, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevUsProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}

	for _, catalog := range GetUnityCatalogRegistry(config) {
		if catalog.Owner.TeamName == "" {
			errors = append(errors, fmt.Sprintf("Catalog %s has no owning team; please set one.", catalog.Name))
		}
		if catalog.UnmanagedPermissions {
			for _, schema := range catalog.Schemas {
				if len(schema.CanReadGroups) > 0 || len(schema.CanReadWriteGroups) > 0 {
					errors = append(errors, fmt.Sprintf("Catalog %s has been marked as unmanaged, but contains user permissions on schema %s. Please remove one.", catalog.Name, schema.Name))
				}
			}
		}
	}

	if len(errors) > 0 {
		t.Errorf("Catalog configuration errors: %v", errors)
	}
}

func TestDatabricksGroupToUnityCatalogAssetPermissions(t *testing.T) {
	databricksGroupToCanReadDatabases := make(map[string][]string)
	databricksGroupToCanWriteDatabases := make(map[string][]string)
	biztechGroupToCanReadDatabaseRegions := make(map[string]map[string][]string)
	biztechGroupToCanWriteDatabaseRegions := make(map[string]map[string][]string)

	config, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevUsProviderGroup)
	if err != nil {
		t.Fatalf("failed to get databricks config: %s", err)
	}

	allDatabases := databaseregistries.GetAllDatabases(config, databaseregistries.AllUnityCatalogDatabases)

	for _, db := range allDatabases {
		databaseNameFullPath := fmt.Sprintf("default.%s", db.Name)

		for _, group := range db.CanReadGroups {
			groupName := group.DatabricksAccountGroupName()
			databricksGroupToCanReadDatabases[groupName] = append(databricksGroupToCanReadDatabases[groupName], databaseNameFullPath)
		}
		for _, group := range db.CanReadWriteGroups {
			groupName := group.DatabricksAccountGroupName()
			databricksGroupToCanReadDatabases[groupName] = append(databricksGroupToCanReadDatabases[groupName], databaseNameFullPath)
			databricksGroupToCanWriteDatabases[groupName] = append(databricksGroupToCanWriteDatabases[groupName], databaseNameFullPath)
		}
		for groupName, regions := range db.CanReadBiztechGroups {
			addDBRegionMapping(biztechGroupToCanReadDatabaseRegions, groupName, databaseNameFullPath, regions)
		}
		for groupName, regions := range db.CanReadWriteBiztechGroups {
			addDBRegionMapping(biztechGroupToCanReadDatabaseRegions, groupName, databaseNameFullPath, regions)
			addDBRegionMapping(biztechGroupToCanWriteDatabaseRegions, groupName, databaseNameFullPath, regions)
		}

		// Also add the owner team to the can read and write databases
		ownerTeamName := db.OwnerTeamName
		if ownerTeamName == "" {
			ownerTeamName = db.OwnerTeam.DatabricksAccountGroupName()
		}
		databricksGroupToCanReadDatabases[ownerTeamName] = append(databricksGroupToCanReadDatabases[ownerTeamName], databaseNameFullPath)
		databricksGroupToCanWriteDatabases[ownerTeamName] = append(databricksGroupToCanWriteDatabases[ownerTeamName], databaseNameFullPath)
	}

	// For the non_govramp_customer_data catalog, we need to add the tables that each team can read.
	// For this catalog we support table level access control.
	nonGovRampCustomerDataTables := govramp.GetAllNonGovrampTables()
	databricksGroupToNonGovRampTablesCanRead := make(map[string][]string)
	for _, table := range nonGovRampCustomerDataTables {
		fullTableName := fmt.Sprintf("non_govramp_customer_data.%s.%s", table.SourceSchema, table.SourceTable)

		// Apply special-case logic: if a table grants access to all GovRamp registry teams,
		// then every group that appears anywhere in the registry should be granted read access.
		if table.GrantAccessToAllGovrampRegistryTeams {
			allGroupsSet := make(map[string]struct{})
			for _, t := range govramp.GetAllNonGovrampTables() {
				for _, g := range t.CanReadGroups {
					allGroupsSet[g.DatabricksAccountGroupName()] = struct{}{}
				}
				for _, g := range t.CanReadBiztechGroups {
					allGroupsSet[g] = struct{}{}
				}
			}
			for groupName := range allGroupsSet {
				databricksGroupToNonGovRampTablesCanRead[groupName] = append(databricksGroupToNonGovRampTablesCanRead[groupName], fullTableName)
			}
			continue
		}

		for _, group := range table.CanReadGroups {
			groupName := group.DatabricksAccountGroupName()
			databricksGroupToNonGovRampTablesCanRead[groupName] = append(databricksGroupToNonGovRampTablesCanRead[groupName], fullTableName)
		}
		for _, group := range table.CanReadBiztechGroups {
			groupName := group
			databricksGroupToNonGovRampTablesCanRead[groupName] = append(databricksGroupToNonGovRampTablesCanRead[groupName], fullTableName)
		}
	}

	// Snapshot the databases that each team can read and the individual tables that each team can read.
	// Create a combined set of all teams from both database and table permissions
	allTeamsCanRead := make(map[string]struct{})
	for team := range databricksGroupToCanReadDatabases {
		allTeamsCanRead[team] = struct{}{}
	}
	for team := range biztechGroupToCanReadDatabaseRegions {
		allTeamsCanRead[team] = struct{}{}
	}
	for team := range databricksGroupToNonGovRampTablesCanRead {
		allTeamsCanRead[team] = struct{}{}
	}

	teamsCanReadSorted := make([]string, 0, len(allTeamsCanRead))
	for team := range allTeamsCanRead {
		teamsCanReadSorted = append(teamsCanReadSorted, team)
	}
	sort.Strings(teamsCanReadSorted)

	for _, team := range teamsCanReadSorted {
		testName := fmt.Sprintf("Team:%s: assets can read", team)
		t.Run(testName, func(t *testing.T) {
			databases, databaseRegions := getDatabaseAccess(team, databricksGroupToCanReadDatabases, biztechGroupToCanReadDatabaseRegions)

			tables := uniqueSorted(databricksGroupToNonGovRampTablesCanRead[team])

			snapshotter := snapshotter.New(t)
			defer snapshotter.Verify()

			var databasesSnapshotEntry any
			if len(databaseRegions) == 0 {
				// Preserve existing snapshot shape for non-biztech groups.
				entry := make(map[string][]string)
				entry["databases"] = databases
				databasesSnapshotEntry = entry
			} else {
				// Biztech groups: snapshot only the db->regions mapping (databases list is redundant).
				entry := make(map[string]any)
				entry["databaseRegions"] = databaseRegions
				databasesSnapshotEntry = entry
			}

			tablesSnapshotEntry := make(map[string][]string)
			tablesSnapshotEntry["tables"] = tables

			snapshotter.Snapshot(fmt.Sprintf("%s: assets can read", team), databasesSnapshotEntry, tablesSnapshotEntry)
		})
	}

	// Snapshot the databases that each team can write to.
	// We don't support table level access control for writes.
	allTeamsCanWrite := make(map[string]struct{})
	for team := range databricksGroupToCanWriteDatabases {
		allTeamsCanWrite[team] = struct{}{}
	}
	for team := range biztechGroupToCanWriteDatabaseRegions {
		allTeamsCanWrite[team] = struct{}{}
	}

	teamsCanWriteSorted := make([]string, 0, len(allTeamsCanWrite))
	for team := range allTeamsCanWrite {
		teamsCanWriteSorted = append(teamsCanWriteSorted, team)
	}
	sort.Strings(teamsCanWriteSorted)

	for _, team := range teamsCanWriteSorted {
		testName := fmt.Sprintf("Team:%s: assets can write", team)
		t.Run(testName, func(t *testing.T) {
			databases, databaseRegions := getDatabaseAccess(team, databricksGroupToCanWriteDatabases, biztechGroupToCanWriteDatabaseRegions)

			snapshotter := snapshotter.New(t)
			defer snapshotter.Verify()

			var databasesSnapshotEntry any
			if len(databaseRegions) == 0 {
				// Preserve existing snapshot shape for non-biztech groups.
				entry := make(map[string][]string)
				entry["databases"] = databases
				databasesSnapshotEntry = entry
			} else {
				// Biztech groups: snapshot only the db->regions mapping (databases list is redundant).
				entry := make(map[string]any)
				entry["databaseRegions"] = databaseRegions
				databasesSnapshotEntry = entry
			}

			snapshotter.Snapshot(fmt.Sprintf("%s: assets can write", team), databasesSnapshotEntry)
		})
	}

}

func addDBRegionMapping(m map[string]map[string][]string, groupName, database string, regions []string) {
	if len(regions) == 0 {
		return
	}
	if _, ok := m[groupName]; !ok {
		m[groupName] = make(map[string][]string)
	}
	// Merge with existing regions instead of overwriting
	existing := m[groupName][database]
	m[groupName][database] = uniqueSorted(append(existing, regions...))
}

func getDatabaseAccess(
	team string,
	groupToDatabases map[string][]string,
	biztechGroupToDatabaseRegions map[string]map[string][]string,
) ([]string, map[string][]string) {
	// Default: normal (non-biztech) groups use the legacy list-of-databases view.
	databases := uniqueSorted(groupToDatabases[team])

	// Biztech groups: provide both a list view and a db->regions mapping.
	dbRegions := biztechGroupToDatabaseRegions[team]
	if len(dbRegions) == 0 {
		return databases, map[string][]string{}
	}

	if len(databases) == 0 {
		keys := make([]string, 0, len(dbRegions))
		for db := range dbRegions {
			keys = append(keys, db)
		}
		sort.Strings(keys)
		databases = keys
	}

	// Ensure deterministic region ordering per database.
	out := make(map[string][]string, len(dbRegions))
	for db, regions := range dbRegions {
		out[db] = uniqueSorted(regions)
	}
	return databases, out
}
