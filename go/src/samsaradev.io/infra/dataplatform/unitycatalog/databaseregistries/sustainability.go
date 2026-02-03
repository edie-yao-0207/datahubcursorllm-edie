package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var SustainabilityDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "ecodriving_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.Sustainability,
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
	{
		Name:          "engine_state",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.Sustainability,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "engine_state_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.Sustainability,
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
		Name:          "ev_charging_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.Sustainability,
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
		Name:          "evsandecodriving_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.Sustainability,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.Sustainability,
		},
	},
	{
		Name:          "fuel_energy_efficiency_report",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.Sustainability,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		SkipGlueCatalog: true,
		CanReadGroups:   clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-csops-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "pto_input",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.Sustainability,
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
		Name:          "sustainability_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.Sustainability,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.Sustainability,
		},
	},
}
