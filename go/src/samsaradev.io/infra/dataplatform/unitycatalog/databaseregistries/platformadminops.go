package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var PlatformAdminOpsDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "platformadminops_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.PlatformAdminOps,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.PlatformAdminOps,
		},
	},
}
