package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

var CloudDatabases = []SamsaraDB{ // lint: +sorted
	{
		// Delete this entry once we move off of Glue. This is a legacy database entry, and the db file in the
		// databricks-warehouse bucket is empty. The tables for this db actually live in rds-delta-lake bucket,
		// and the database registry entry for UC is maintained in the rdsdeltalake/registry.go file.
		Name:          "clouddb",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.Cloud,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
}
