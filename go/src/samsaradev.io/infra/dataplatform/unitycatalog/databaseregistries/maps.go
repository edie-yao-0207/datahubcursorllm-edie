package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var MapsDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "dataprep_maps",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.Maps,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.Maps,
		},
	},
	{
		Name:          "dataprep_place_migration",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.Maps,
		DatabaseGroup: ProductionDatabaseGroup,
		AWSAccountIDs: []string{
			DatabricksAWSAccountUS,
			DatabricksAWSAccountEU,
			DatabricksAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.Maps,
		},
	},
	{
		Name:          "maps_data",
		Bucket:        MapsBucket,
		OwnerTeam:     team.Maps,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: []components.TeamInfo{},
		CanReadWriteGroups: []components.TeamInfo{
			team.Maps,
		},
	},
	{
		Name:          "maps_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.Maps,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.Maps,
		},
	},
}
