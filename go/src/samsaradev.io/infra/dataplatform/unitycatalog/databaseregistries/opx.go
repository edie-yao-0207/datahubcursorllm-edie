package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var OPXDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "aws_cost_drivers",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.OPX,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:            "fivetran_clip_stillness_staging",
		Bucket:          FivetranIncidentioDeltaTablesBucket,
		OwnerTeam:       team.OPX,
		DatabaseGroup:   FivetranDatabaseGroup,
		SkipGlueCatalog: true,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
			team.OPX,
		},
	},
	{
		Name:          "fivetran_launchdarkly_bronze",
		Bucket:        DataPlatformFivetranBronzeBucket,
		OwnerTeam:     team.OPX,
		DatabaseGroup: FivetranDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
		},
		SkipGlueCatalog: true,
		CanReadGroups: []components.TeamInfo{
			team.DataPlatform,
			team.DataEngineering,
			team.DataAnalytics,
			team.DecisionScience,
			team.ProductManagement,
			team.CoreServices,
			team.TPM,
			team.Firmware,
			team.FirmwareAutomotiveEngineering,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.OPX,
			team.DataTools,
		},
	},
	{
		Name:          "incidentio_bronze",
		Bucket:        FivetranIncidentioDeltaTablesBucket,
		OwnerTeam:     team.OPX,
		DatabaseGroup: FivetranDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.OPX,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "opx",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.OPX,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.OPX,
		},
	},
	{
		Name:          "pagerduty_bronze",
		Bucket:        FivetranPagerDutyDeltaTablesBucket,
		OwnerTeam:     team.OPX,
		DatabaseGroup: FivetranDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.OPX,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:               "perf_infra",
		Bucket:             DataPipelinesDeltaLakeBucket,
		LegacyGlueS3Bucket: DatabricksWarehouseBucket,
		OwnerTeam:          team.OPX,
		DatabaseGroup:      ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.SRE,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
}
