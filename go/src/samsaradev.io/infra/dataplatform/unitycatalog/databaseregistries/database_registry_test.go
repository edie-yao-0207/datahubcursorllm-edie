package databaseregistries

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/datapipelines/configvalidator"
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/testloader"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"

	"os"
	"path/filepath"
)

// Restrict access to any schemas in this list to only Data Platform/admins. No other teams should have access.
var restrictedSchemas = map[string]struct{}{}

// Snapshot all database names. Data Platform review is enforced for any
// database additions or deletions that would change this snapshot.
func TestDBNames(t *testing.T) {
	var env struct {
		Snap *snapshotter.Snapshotter
	}
	testloader.MustStart(t, &env)

	config, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevUsProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}

	dbNames := GetAllDBNamesSorted(config, AllUnityCatalogDatabases)
	env.Snap.Snapshot("deltalake-database-list", dbNames)
}

// No new databases should be added to this bucket. Validate that the list of legacy DBs in the
// Playground bucket does not change.
func TestPlaygroundBucketDBNames(t *testing.T) {
	config, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevUsProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}

	// Do not add more DBs to this list.
	existingLegacyPlaygroundDbs := []string{ // lint: +sorted
		"adasmetrics",
		"apollo",
		"baxter",
		"bigquery",
		"cm3xinvestigation",
		"compliance_metrics",
		"connectedworker",
		"databricks_alerts",
		"dataproducts",
		"devexp",
		"firebase_crashlytics",
		"firmwarerelease",
		"growth",
		"gtms",
		"helpers",
		"ksdifftool",
		"mdm",
		"mldatasets",
		"mobile_metrics",
		"multicam",
		"stopsigns",
		"vgdiagnostics",
	}

	var playgroundBucketDbNames []string
	for _, db := range GetAllDatabases(config, AllUnityCatalogDatabases) {
		if db.Bucket == LegacyDatabricksPlaygroundBucket_DONOTUSE || db.LegacyGlueS3Bucket == LegacyDatabricksPlaygroundBucket_DONOTUSE {
			playgroundBucketDbNames = append(playgroundBucketDbNames, db.Name)
		}
	}
	sort.Strings(playgroundBucketDbNames)
	assert.Equal(t, existingLegacyPlaygroundDbs, playgroundBucketDbNames)
}

func TestValidateDatabaseRegistry(t *testing.T) {
	config, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevUsProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}

	var allErrors []string
	databaseSet := make(map[string]struct{})
	for _, db := range GetAllDatabases(config, AllUnityCatalogDatabases) {
		var errors []string

		if db.Name == "" {
			errors = append(errors, "Name is missing")
		} else if _, exists := databaseSet[db.Name]; exists {
			errors = append(errors, fmt.Sprintf("Duplicate database: %s", db.Name))
		}
		databaseSet[db.Name] = struct{}{}

		if db.OwnerTeam.Name() == "" && db.OwnerTeamName == "" {
			errors = append(errors, "OwnerTeam or OwnerTeamName is missing")
		}
		if db.OwnerTeam.Name() != "" && db.OwnerTeamName != "" {
			errors = append(errors, "OwnerTeamName does not match OwnerTeam.TeamName. No need to set OwnerTeamName if OwnerTeam is set")
		}
		if db.Bucket == "" {
			errors = append(errors, "Bucket is missing")
		}
		if len(db.AWSAccountIDs) == 0 {
			errors = append(errors, "AWSAccountIDs is empty")
		} else {
			// AWS Account ID must be valid.
			for _, id := range db.AWSAccountIDs {
				if _, ok := infraconsts.AllSamsaraAWSAccountIDsDefaultRegionPrefix[id]; !ok {
					errors = append(errors, fmt.Sprintf("Invalid AWSAccountID: %s", id))
				}
			}
		}

		writeGroupsMap := make(map[string]struct{})
		for _, group := range db.CanReadWriteGroups {
			if _, ok := writeGroupsMap[group.TeamName]; ok {
				errors = append(errors, fmt.Sprintf("Duplicate group in CanReadWriteGroups: %s", group.TeamName))
				continue
			}
			// Disallow access to restricted schemas to all teams except Data Platform.
			_, isRestrictedSchema := restrictedSchemas[db.Name]
			if group.TeamName != team.DataPlatform.TeamName && isRestrictedSchema {
				errors = append(errors, fmt.Sprintf("Access to %s by team %s not allowed", db.Name, group.TeamName))
				continue
			}
			writeGroupsMap[group.TeamName] = struct{}{}
		}
		// Validate there are no duplicate teams added *within* CanReadGroups, as well as no duplicate teams that
		// are already added to CanReadWriteGroups (as read/write permission already grants read permissions).
		// Skip this validation if CanReadGroups is ALL TEAMS (i.e. allow duplicate teams between CanReadGroups and CanReadWriteGroups).
		// We set this with the clusterhelpers.DatabricksClusterTeams() and it would be unneccessarily verbose if we listed out every team
		// *except* the ones with write permissions.
		readGroupsMap := make(map[string]struct{})
		if !assert.ObjectsAreEqual(clusterhelpers.DatabricksClusterTeams(), db.CanReadGroups) {
			for _, group := range db.CanReadGroups {
				if _, ok := readGroupsMap[group.TeamName]; ok {
					errors = append(errors, fmt.Sprintf("Duplicate group in CanReadGroups: %s", group.TeamName))
					continue
				}
				if _, ok := writeGroupsMap[group.TeamName]; ok {
					errors = append(errors, fmt.Sprintf("Group %s already in CanReadWriteGroups. Does not need to be listed in CanReadGroups", group.TeamName))
				}
				// Disallow access to restricted schemas to all teams except Data Platform.
				_, isRestrictedSchema := restrictedSchemas[db.Name]
				if group.TeamName != team.DataPlatform.TeamName && isRestrictedSchema {
					errors = append(errors, fmt.Sprintf("Access to %s by team %s not allowed", db.Name, group.TeamName))
					continue
				}
				readGroupsMap[group.TeamName] = struct{}{}
			}
		}

		if len(errors) > 0 {
			allErrors = append(allErrors, fmt.Sprintf("'%s': %s", db.Name, strings.Join(errors, ", ")))
		}
	}

	if len(allErrors) > 0 {
		t.Errorf("Configuration errors for databases:\n%s", strings.Join(allErrors, "\n"))
	}
}

func TestGetRegionsMap(t *testing.T) {
	getRegionMapKeys := func(regionMap map[string]bool) (regionList []string) {
		for region := range regionMap {
			regionList = append(regionList, region)
		}
		return regionList
	}

	usOnlyDatabase := SamsaraDB{
		Name:          "us_test",
		AWSAccountIDs: []string{strconv.Itoa(infraconsts.SamsaraAWSAccountID)},
	}
	assert.Equal(t, []string{infraconsts.SamsaraAWSDefaultRegion}, getRegionMapKeys(usOnlyDatabase.GetRegionsMap()))

	euOnlyDatabase := SamsaraDB{
		Name:          "eu_test",
		AWSAccountIDs: []string{infraconsts.SamsaraAWSEUDataScienceAccountID},
	}
	assert.Equal(t, []string{infraconsts.SamsaraAWSEURegion}, getRegionMapKeys(euOnlyDatabase.GetRegionsMap()))

	usAndEuOnlyDatabase := SamsaraDB{
		Name:          "us_and_eu_test",
		AWSAccountIDs: []string{strconv.Itoa(infraconsts.SamsaraAWSDatabricksAccountID), strconv.Itoa(infraconsts.SamsaraAWSEUDatabricksAccountID)},
	}
	usEuRegions := getRegionMapKeys(usAndEuOnlyDatabase.GetRegionsMap())
	sort.Strings(usEuRegions)
	assert.Equal(t, []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSDefaultRegion}, usEuRegions)
}

func TestGetBucketsForDatabasesInRegion(t *testing.T) {
	usConfig, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevUsProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}
	euConfig, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevEuProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}

	// We will need to update this test if any of the following databases get deleted or the regions are modified.
	dbNames := []string{
		"dataplatform_dev", // databricks-workspace; US & EU
		"clouddb",          // databricks-warehouse; US & EU
		"dataplatform",     // databricks-warehouse (Duplicate should not appear in the final list); US & EU
		"netsuite_finance", // netsuite-finance; US only
	}

	// US Region read buckets.
	expectedReadBucketsUS := []string{
		"samsara-databricks-warehouse",
		"samsara-databricks-workspace",
		"samsara-netsuite-finance",
	}
	sort.Strings(expectedReadBucketsUS)
	readBucketsUS := GetBucketsForDatabasesInRegion(usConfig, dbNames, Read, LegacyOnlyDatabases)
	sort.Strings(readBucketsUS)
	assert.Equal(t, expectedReadBucketsUS, readBucketsUS)

	// US Region readwrite buckets. Expect specific file access for
	// dataplatform_dev db instead of blanket write access to databricks-workspace.
	expectedReadWriteBucketsUS := []string{
		"samsara-databricks-warehouse",
		"samsara-databricks-workspace/team_dbs/dataplatform_dev.db",
		"samsara-netsuite-finance",
	}
	sort.Strings(expectedReadWriteBucketsUS)
	readWriteBucketsUS := GetBucketsForDatabasesInRegion(usConfig, dbNames, ReadWrite, LegacyOnlyDatabases)
	sort.Strings(readWriteBucketsUS)
	assert.Equal(t, expectedReadWriteBucketsUS, readWriteBucketsUS)

	// EU Region read buckets.
	expectedReadBucketsEU := []string{
		"samsara-eu-databricks-warehouse",
		"samsara-eu-databricks-workspace",
	}
	sort.Strings(expectedReadBucketsEU)
	readBucketsEU := GetBucketsForDatabasesInRegion(euConfig, dbNames, Read, LegacyOnlyDatabases)
	sort.Strings(readBucketsEU)
	assert.Equal(t, expectedReadBucketsEU, readBucketsEU)
}

// Validate that there is an entry in the database registry for each data pipelines output database.
func TestDataPipelinesDbs(t *testing.T) {
	config, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevUsProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}

	// Make a lookup map of all DBs defined in the database registry.
	registryDbNames := make(map[string]struct{})
	for _, dbname := range GetAllDBNamesSorted(config, AllUnityCatalogDatabases) {
		registryDbNames[dbname] = struct{}{}
	}

	// Scrape all the Data Pipelines output schema names from the .json node metadata files.
	undefinedDataPipelineDbs := make(map[string]struct{})
	err = filepath.Walk(configvalidator.TransformationDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || filepath.Ext(path) != ".json" || strings.HasSuffix(path, ".test.json") {
			return nil
		}

		// Read the json file and parse into the expected config format.
		fileBytes, err := os.ReadFile(path)
		assert.NoError(t, err)
		var nodeConfig configvalidator.NodeConfiguration
		err = json.Unmarshal(fileBytes, &nodeConfig)
		assert.NoError(t, err)

		outputDbName := nodeConfig.Output.DAGName()
		owner := nodeConfig.Owner.TeamName

		// All data pipeline nodes should have an owner.
		assert.False(t, owner == "")
		// All data pipeline nodes should have an output schema defined.
		assert.False(t, outputDbName == "")

		// Track all data pipeline output databases that do not have an entry in the database registry.
		if _, ok := registryDbNames[outputDbName]; !ok {
			undefinedDataPipelineDbs[outputDbName] = struct{}{}
		}
		return nil
	})
	assert.NoError(t, err)

	// Expect every data pipelines output DB to have a database registry entry.
	assert.Empty(t, undefinedDataPipelineDbs)
}
