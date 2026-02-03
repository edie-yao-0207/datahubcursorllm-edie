package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

var AssetFoundationsDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "activity_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.AssetFoundations,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":            {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":      {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "start_stop_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.AssetFoundations,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "time_on_site_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.AssetFoundations,
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
		Name:          "trip_history_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.AssetFoundations,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
	},
}
