package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var MobileDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "driver_app_engagement_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.Mobile,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "firebase_crashlytics",
		Bucket:        LegacyDatabricksPlaygroundBucket_DONOTUSE,
		OwnerTeam:     team.Mobile,
		DatabaseGroup: PlaygroundDatabaseGroup_LEGACY,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "mobile_logs",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.Mobile,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DataScience,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-csops-developers":  {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "mobile_metrics",
		Bucket:        LegacyDatabricksPlaygroundBucket_DONOTUSE,
		OwnerTeam:     team.Mobile,
		DatabaseGroup: PlaygroundDatabaseGroup_LEGACY,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
}
