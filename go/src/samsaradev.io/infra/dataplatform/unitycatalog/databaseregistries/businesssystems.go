package databaseregistries

import (
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var BusinessSystemsDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "fivetran_netsuite_finance",
		Bucket:        NetsuiteFinanceBucket,
		OwnerTeam:     team.BusinessSystems,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		SkipGlueCatalog: true,
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.FinancialOperations,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
			team.BusinessSystems,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-finance": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "netsuite_finance",
		Bucket:        NetsuiteFinanceBucket,
		OwnerTeam:     team.BusinessSystems,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.FinancialOperations,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
			team.BusinessSystems,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-finance":                {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "netsuite_suiteanalytics",
		Bucket:        NetsuiteFinanceBucket,
		OwnerTeam:     team.BusinessSystems,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
		},
		SkipGlueCatalog: true,
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BusinessSystems,
			team.FinancialOperations,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
			team.PlatformOperations,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-finance":                {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
}
