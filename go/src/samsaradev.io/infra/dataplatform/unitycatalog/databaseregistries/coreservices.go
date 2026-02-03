package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var CoreServicesDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "coreservices_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.CoreServices,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.CoreServices,
		},
	},
}
