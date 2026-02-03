package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var SafetyEventReviewDevDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "safetyeventreviewdev_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.SafetyEventReviewDev,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.SafetyEventReviewDev,
		},
	},
}
