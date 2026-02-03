package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var OemDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "oem_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.Oem,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.Oem,
		},
	},
}
