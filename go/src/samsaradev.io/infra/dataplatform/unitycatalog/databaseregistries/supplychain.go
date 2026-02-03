package databaseregistries

import (
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var SupplyChainDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "supplychain",
		Bucket:        SupplychainBucket,
		OwnerTeamName: "bt-supplychain",
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.SupplyChain,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-supplychain": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
}
