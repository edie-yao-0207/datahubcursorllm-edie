package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var SafetyRiskDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "risk_overview",
		Bucket:        DataPipelinesDeltaLakeBucket,
		OwnerTeam:     team.SafetyRisk,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.SafetyRisk,
		},
	},
}
