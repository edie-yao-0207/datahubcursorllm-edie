package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var MLCVDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name: "dataprep_ml",
		// Has table data in samsara-databricks-warehouse and samsara-data-pipelines-delta-lake.
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.MLCV,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DataScience,
		},
	},
	{
		Name:          "mldatasets",
		Bucket:        LegacyDatabricksPlaygroundBucket_DONOTUSE,
		OwnerTeam:     team.MLCV,
		DatabaseGroup: PlaygroundDatabaseGroup_LEGACY,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
}
