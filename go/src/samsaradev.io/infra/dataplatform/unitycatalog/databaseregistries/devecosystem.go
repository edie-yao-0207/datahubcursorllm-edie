package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var DevEcosystemDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "api_logs",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DevEcosystem,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DataPlatform,
			team.DataScience,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "api_logs_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.DevEcosystem,
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
		Name:          "devecosystem",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.DevEcosystem,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DevEcosystem,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "devecosystem_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.DevEcosystem,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DevEcosystem,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "fivetranconnectordemo",
		Bucket:        FivetranSamsaraSourceQaBucket,
		OwnerTeam:     team.DevEcosystem,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		SkipGlueCatalog: true,
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
			team.BizTechEnterpriseDataSensitive,
			team.BusinessSystems,
			team.BusinessSystemsRecruiting,
			team.DataScience,
			team.DevEcosystem,
			team.OPX,
			team.PlatformOperations,
		},
	},
	{
		Name:          "fivetranconnectortest",
		Bucket:        FivetranSamsaraSourceQaBucket,
		OwnerTeam:     team.DevEcosystem,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		SkipGlueCatalog: true,
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
			team.BizTechEnterpriseDataSensitive,
			team.BusinessSystems,
			team.BusinessSystemsRecruiting,
			team.DataScience,
			team.DevEcosystem,
			team.OPX,
			team.PlatformOperations,
		},
	},
}
