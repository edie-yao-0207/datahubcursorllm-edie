package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var ComplianceDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "compliance",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.Compliance,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.Compliance,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "compliance_metrics",
		Bucket:        LegacyDatabricksPlaygroundBucket_DONOTUSE,
		OwnerTeam:     team.Compliance,
		DatabaseGroup: PlaygroundDatabaseGroup_LEGACY,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
}
