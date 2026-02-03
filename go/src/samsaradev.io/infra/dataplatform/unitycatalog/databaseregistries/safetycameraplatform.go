package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var SafetyCameraPlatformDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "safetycameraplatform_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.SafetyCameraPlatform,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.SafetyCameraPlatform,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "video_quality_metrics",
		OwnerTeam:     team.SafetyCameraPlatform,
		Bucket:        DatabricksWorkspaceBucket,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.SafetyCameraPlatform,
			team.Firmware,
		},
	},
}
