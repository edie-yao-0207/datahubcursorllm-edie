package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var SafetyEventIngestionAndFilteringDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "safety_map_data",
		Bucket:        SafetyMapDataSourcesBucket,
		OwnerTeam:     team.SafetyEventIngestionAndFiltering,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: []components.TeamInfo{
			team.DataAnalytics,
			team.FirmwareVdp,
			team.Routing,
			team.SafetyFirmware,
			team.DataEngineering,
			team.CoreServices,
			team.Sustainability,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.DataScience,
			team.SafetyEventIngestionAndFiltering,
			team.SafetyPlatform,
			team.Maps,
			team.TPM,
		},
	},
	{
		Name:          "safetyeventingestionandfiltering_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.SafetyEventIngestionAndFiltering,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.SafetyEventIngestionAndFiltering,
		},
	},
	{
		Name:          "speeding_incident_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.SafetyEventIngestionAndFiltering,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
}
