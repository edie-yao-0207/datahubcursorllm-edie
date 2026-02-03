package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var ProductOperationsDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "productoperations_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.ProductOperations,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.ProductOperations,
		},
	},
}
