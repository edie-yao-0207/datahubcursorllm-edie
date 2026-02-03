package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

// Reference: https://github.com/samsara-dev/backend/pull/218456
var DriverManagementDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "drivermanagement_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.DriverManagement,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DriverManagement,
		},
	},
}
