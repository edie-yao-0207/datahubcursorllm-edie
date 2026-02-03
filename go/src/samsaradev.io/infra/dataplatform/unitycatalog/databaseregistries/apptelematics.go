package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

var AppTelematicsDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:               "apptelematics",
		Bucket:             DataPipelinesDeltaLakeBucket,
		LegacyGlueS3Bucket: DatabricksWarehouseBucket,
		OwnerTeam:          team.AppTelematics,
		DatabaseGroup:      ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
}
