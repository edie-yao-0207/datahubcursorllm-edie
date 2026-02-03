package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var SafetyPlatformDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "baxter",
		Bucket:        LegacyDatabricksPlaygroundBucket_DONOTUSE,
		OwnerTeam:     team.SafetyPlatform,
		DatabaseGroup: PlaygroundDatabaseGroup_LEGACY,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "dataprep_safety",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.SafetyPlatform,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DataEngineering,
			team.DataScience,
			team.SafetyPlatform,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "safetyplatform_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.SafetyPlatform,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.SafetyPlatform,
		},
	},
}
