package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var PlatformReportsDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "platformreports_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.PlatformReports,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.PlatformReports,
		},
	},
}
