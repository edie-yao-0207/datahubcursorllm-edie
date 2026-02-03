package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var FleetSecDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "fleetsec_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.FleetSec,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.FleetSec,
		},
	},
}
