package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
)

var MarketingDataAnalyticsDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "biztech_muhlfelder",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeamName: "bt-mda-analytics",
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		// TODO: Check if giving read/write access to all teams is intentional.
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "biztech_rgeissler",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeamName: "bt-mda-analytics",
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		// TODO: Check if giving read/write access to all teams is intentional.
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "biztech_sbourland",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeamName: "bt-mda-analytics",
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		// TODO: Check if giving read/write access to all teams is intentional.
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "biztech_twalters",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeamName: "bt-mda-analytics",
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		// TODO: Check if giving read/write access to all teams is intentional.
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "biztech_vrao",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeamName: "bt-mda-analytics",
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		// TODO: Check if giving read/write access to all teams is intentional.
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "marketingdata",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeamName: "bt-mda-analytics",
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "marketingdata_dev",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeamName: "bt-mda-analytics",
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "mda_production",
		Bucket:        MarketingDataAnalyticsBucket,
		OwnerTeamName: "bt-mda-analytics",
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "mda_sandbox",
		Bucket:        MarketingDataAnalyticsBucket,
		OwnerTeamName: "bt-mda-analytics",
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "mda_stage",
		Bucket:        MarketingDataAnalyticsBucket,
		OwnerTeamName: "bt-mda-analytics",
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
	},
}
