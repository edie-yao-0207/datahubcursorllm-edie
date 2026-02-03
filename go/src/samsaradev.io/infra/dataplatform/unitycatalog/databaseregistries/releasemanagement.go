package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var ReleaseManagementDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "release_management",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.ReleaseManagement,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.ReleaseManagement,
		},
	},
	{
		Name:          "releasemanagement_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.ReleaseManagement,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.ReleaseManagement,
		},
	},
}
