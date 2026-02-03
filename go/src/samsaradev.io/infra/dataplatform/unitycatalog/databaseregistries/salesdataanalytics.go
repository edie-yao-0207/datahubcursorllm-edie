package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var SalesDataAnalyticsDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "biztech_yagrawal",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.SalesDataAnalytics,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "sda_gold",
		Bucket:        BiztechSdaProductionBucket,
		OwnerTeam:     team.SalesDataAnalytics,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
			team.BizTechEnterpriseDataSensitive,
			team.SalesDataAnalytics,
		},
	},
}
