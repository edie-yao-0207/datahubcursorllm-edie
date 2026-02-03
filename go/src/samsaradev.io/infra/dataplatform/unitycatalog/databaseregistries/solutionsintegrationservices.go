package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var SolutionsIntegrationServicesDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "solutions_integration_services",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeamName: "bt-cs-sis",
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.SalesEngineering,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-migration-salesengineering": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
}
