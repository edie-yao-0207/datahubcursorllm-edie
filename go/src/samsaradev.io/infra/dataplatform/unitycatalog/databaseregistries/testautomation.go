package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var TestAutomationDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "owltomation_results",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.TestAutomation,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.TestAutomation,
		},
	},
	{
		Name:          "testautomation_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.TestAutomation,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.TestAutomation,
		},
	},
}
