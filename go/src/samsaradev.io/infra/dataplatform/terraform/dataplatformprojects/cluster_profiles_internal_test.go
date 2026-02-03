package dataplatformprojects

import (
	"fmt"
	"strings"
	"testing"

	"github.com/samsarahq/go/snapshotter"
	"github.com/stretchr/testify/assert"

	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

// This test exists to assert that we don't let certain buckets get accessed by anyone outside
// of the list maintained in this test. This will prevent us from accidentally giving teams
// permissions to buckets that they shouldn't have access to.
// This is a better alternative than just code review because people may forget or not know which
// buckets have restricted permissions, and we can keep a documented list here with reasons as to why.
func TestRestrictedBuckets(t *testing.T) {

	bucketToTeamMapping := map[string][]string{
		// Only the supply chain & biztech enterprise data team should have access to the supply chain bucket,
		// since it has certain sensitive data.
		"supplychain": {team.SupplyChain.TeamName, team.BizTechEnterpriseData.TeamName, "biztechenterprisedata-admin", "biztech-prod"},
	}

	var errors []string
	config, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevUsProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}

	for _, profile := range allRawClusterProfiles(config) {
		for _, bucket := range append(append([]string{}, profile.readBuckets...), profile.readwriteBuckets...) {
			if teams, ok := bucketToTeamMapping[bucket]; ok {
				found := false
				for _, team := range teams {
					if profile.clusterName == strings.ToLower(team) {
						found = true
						break
					}
				}
				if !found {
					errors = append(errors, fmt.Sprintf("Team %s should not have access to %s bucket.", profile.clusterName, bucket))
				}
			}
		}
	}

	if len(errors) > 0 {
		t.Fatalf("%s\n If this is intentional, please update the test.\n", strings.Join(errors, "\n"))
	}
}

func TestFindIntersectionOfBiztechenterprisedataAndBiztechProdReadWriteDbs(t *testing.T) {
	snapshotter := snapshotter.New(t)
	defer snapshotter.Verify()

	config, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevUsProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}

	biztechProdReadWriteDiff, biztechenterprisedataReadWriteDiff, intersectReadWrite, err := findIntersectionOfBiztechenterprisedataAndBiztechProdReadWriteDbs(config)
	assert.NoError(t, err)

	snapshotter.Snapshot("biztech-prod readwriteDatabases diff", biztechProdReadWriteDiff)
	snapshotter.Snapshot("biztechenterprisedata readwriteDatabases diff", biztechenterprisedataReadWriteDiff)
	snapshotter.Snapshot("intersection of readwriteDatabases between biztech-prod and biztechenterprisedata", intersectReadWrite)
}

func TestFindAndRemoveListIntersection(t *testing.T) {
	list1 := []string{
		"apple",
		"banana",
		"cherry",
		"dragonfruit",
		"eggplant",
	}
	list2 := []string{
		"apple",
		"cherry",
		"fig",
		"grape",
	}

	expectedList1Diff := []string{
		"banana",
		"dragonfruit",
		"eggplant",
	}
	expectedList2Diff := []string{
		"fig",
		"grape",
	}
	expectedIntersectList := []string{
		"apple",
		"cherry",
	}

	uniqueList1, uniqueList2, intersectList := findAndRemoveListIntersection(list1, list2)
	assert.Equal(t, expectedList1Diff, uniqueList1)
	assert.Equal(t, expectedList2Diff, uniqueList2)
	assert.Equal(t, expectedIntersectList, intersectList)
}

func TestValidateClusterProfiles(t *testing.T) {
	config, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevUsProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}

	var invalidProfiles []string
	for _, prof := range allRawClusterProfiles(config) {
		// clusterName and instanceProfileName must be set for all cluster profile entries.
		if prof.clusterName == "" || prof.instanceProfileName == "" {
			invalidProfiles = append(invalidProfiles, fmt.Sprintf("clusterName: %s, instanceProfileName: %s", prof.clusterName, prof.instanceProfileName))
		}
	}
	if len(invalidProfiles) > 0 {
		t.Errorf("Invalid profiles: both clusterName and instanceProfileName must be set.\n%s", strings.Join(invalidProfiles, "\n"))
	}
}

// Validates that there are no duplicate buckets defined (exluding managed policy permissions attached via policyAttachments).
// You do not need to set bucket specific access if access is granted to a database that lives in that bucket in readDatabases or readwriteDatabases.
func TestDuplicateBucketPermissions(t *testing.T) {
	makeMapFromList := func(l []string) map[string]bool {
		boolMap := make(map[string]bool)
		for _, item := range l {
			boolMap[item] = true
		}
		return boolMap
	}

	stripPrefix := func(buckets []string, prefix string) []string {
		var bucketsNoPrefix []string
		for _, bucket := range buckets {
			bucketsNoPrefix = append(bucketsNoPrefix, strings.TrimPrefix(bucket, prefix))
		}
		return bucketsNoPrefix
	}

	usConfig, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevUsProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}
	euConfig, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevEuProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}
	caConfig, err := dataplatformconfig.DatabricksProviderConfig(dataplatformconfig.DatabricksDevCaProviderGroup)
	if err != nil {
		t.Fatalf("error getting databricks config: %s", err)
	}

	for _, prof := range allRawClusterProfiles(usConfig) {
		// Get the list of all S3 buckets that will be automatically looked up and added from database names.
		dbReadBucketsUS := databaseregistries.GetBucketsForDatabasesInRegion(usConfig, prof.readDatabases, databaseregistries.Read, databaseregistries.LegacyOnlyDatabases)
		dbReadBucketsEU := databaseregistries.GetBucketsForDatabasesInRegion(euConfig, prof.readDatabases, databaseregistries.Read, databaseregistries.LegacyOnlyDatabases)
		dbReadBucketsCA := databaseregistries.GetBucketsForDatabasesInRegion(caConfig, prof.readDatabases, databaseregistries.Read, databaseregistries.LegacyOnlyDatabases)
		dbReadWriteBucketsUS := databaseregistries.GetBucketsForDatabasesInRegion(usConfig, prof.readwriteDatabases, databaseregistries.ReadWrite, databaseregistries.LegacyOnlyDatabases)
		dbReadWriteBucketsEU := databaseregistries.GetBucketsForDatabasesInRegion(euConfig, prof.readwriteDatabases, databaseregistries.ReadWrite, databaseregistries.LegacyOnlyDatabases)
		dbReadWriteBucketsCA := databaseregistries.GetBucketsForDatabasesInRegion(caConfig, prof.readwriteDatabases, databaseregistries.ReadWrite, databaseregistries.LegacyOnlyDatabases)

		// Strip the prefix off of the bucket names.
		dbReadBucketsNoPrefixUS := stripPrefix(dbReadBucketsUS, "samsara-")
		dbReadBucketsNoPrefixEU := stripPrefix(dbReadBucketsUS, "samsara-eu-")
		dbReadBucketsNoPrefixCA := stripPrefix(dbReadBucketsCA, "samsara-ca-")
		dbReadWriteBucketsNoPrefixUS := stripPrefix(dbReadWriteBucketsUS, "samsara-")
		dbReadWriteBucketsNoPrefixEU := stripPrefix(dbReadWriteBucketsEU, "samsara-eu-")
		dbReadWriteBucketsNoPrefixCA := stripPrefix(dbReadWriteBucketsCA, "samsara-ca-")

		// If a bucket is explicitly set in readBuckets, but a DB already automatically adds the bucket in *all* regions (US, EU, and CA),

		// flag it as a duplicate.
		var duplicateReadBuckets []string
		for _, bucket := range prof.readBuckets {
			// Use the lists that have stripped off the bucket prefixes, because prefixes are not set in the readBuckets list.
			if _, usOk := makeMapFromList(dbReadBucketsNoPrefixUS)[bucket]; usOk {
				if _, euOk := makeMapFromList(dbReadBucketsNoPrefixEU)[bucket]; euOk {
					if _, caOk := makeMapFromList(dbReadBucketsNoPrefixCA)[bucket]; caOk {
						duplicateReadBuckets = append(duplicateReadBuckets, bucket)
					}
				}
			}
		}
		assert.Empty(t, duplicateReadBuckets, fmt.Sprintf("duplicate readBuckets: %s", prof.clusterName))

		// If a bucket is explicitly set in regionSpecificReadBuckets, but a DB already automatically adds the bucket in that region,
		// flag it as a duplicate.
		var duplicateRegionSpecificReadBuckets []string
		for region, buckets := range prof.regionSpecificReadBuckets {
			dbReadBucketsList := dbReadBucketsUS
			if region == infraconsts.SamsaraAWSEURegion {
				dbReadBucketsList = dbReadBucketsEU
			} else if region == infraconsts.SamsaraAWSCARegion {
				dbReadBucketsList = dbReadBucketsCA
			}
			for _, bucket := range buckets {
				// Use the lists that have the bucket prefixes, because prefixes *are* set in the region specific bucket lists.
				if _, ok := makeMapFromList(dbReadBucketsList)[bucket]; ok {
					duplicateRegionSpecificReadBuckets = append(duplicateRegionSpecificReadBuckets, bucket)
				}
			}
		}
		assert.Empty(t, duplicateRegionSpecificReadBuckets, fmt.Sprintf("duplicate regionSpecificReadBuckets: %s", prof.clusterName))

		// If a bucket is explicitly set in readwriteBuckets, but a DB already automatically adds the bucket in *all* regions (US, EU, and CA),
		// flag it as a duplicate.
		var duplicateReadWriteBuckets []string
		for _, bucket := range prof.readwriteBuckets {
			// Use the lists that have stripped off the bucket prefixes, because prefixes are not set in the readwriteBuckets list.
			if _, usOk := makeMapFromList(dbReadWriteBucketsNoPrefixUS)[bucket]; usOk {
				if _, euOk := makeMapFromList(dbReadWriteBucketsNoPrefixEU)[bucket]; euOk {
					if _, caOk := makeMapFromList(dbReadWriteBucketsNoPrefixCA)[bucket]; caOk {
						duplicateReadWriteBuckets = append(duplicateReadWriteBuckets, bucket)
					}
				}
			}
		}
		assert.Empty(t, duplicateReadWriteBuckets, fmt.Sprintf("duplicate readwriteBuckets: %s", prof.clusterName))

		// If a bucket is explicitly set in regionSpecificReadWriteBuckets, but a DB already automatically adds the bucket in that region,
		// flag it as a duplicate.
		var duplicateRegionSpecificReadWriteBuckets []string
		for region, buckets := range prof.regionSpecificReadWriteBuckets {
			dbReadWriteBucketsList := dbReadWriteBucketsUS
			if region == infraconsts.SamsaraAWSEURegion {
				dbReadWriteBucketsList = dbReadWriteBucketsEU
			} else if region == infraconsts.SamsaraAWSCARegion {
				dbReadWriteBucketsList = dbReadWriteBucketsCA
			}
			for _, bucket := range buckets {
				// Use the lists that have the bucket prefixes, because prefixes *are* set in the region specific bucket lists.
				if _, ok := makeMapFromList(dbReadWriteBucketsList)[bucket]; ok {
					duplicateRegionSpecificReadWriteBuckets = append(duplicateRegionSpecificReadWriteBuckets, bucket)
				}
			}
		}
		assert.Empty(t, duplicateRegionSpecificReadWriteBuckets, fmt.Sprintf("duplicate regionSpecificReadWriteBuckets: %s", prof.clusterName))
	}
}
