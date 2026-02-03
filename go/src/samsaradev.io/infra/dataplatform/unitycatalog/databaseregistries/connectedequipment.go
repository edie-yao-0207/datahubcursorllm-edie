package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var ConnectedEquipmentDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "connectedequipment_dev",
		Bucket:        DatabricksWorkspaceBucket,
		OwnerTeam:     team.ConnectedEquipment,
		DatabaseGroup: TeamDevDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.ConnectedEquipment,
			team.STCEUnpowered,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":            {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":      {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "dataprep_assets",
		Bucket:        DatabricksWarehouseBucket,
		OwnerTeam:     team.ConnectedEquipment,
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
}
