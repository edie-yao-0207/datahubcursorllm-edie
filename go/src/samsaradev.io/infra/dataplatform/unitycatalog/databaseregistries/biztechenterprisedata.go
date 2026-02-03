package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var BizTechEnterpriseDataDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "biztech_agautam",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_accouting_dbx_gold",
		Bucket:        BiztechEdwGoldBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_adaptive_bronze",
		Bucket:        BiztechEdwSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_assembled_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_assembled_silver",
		Bucket:        BiztechEdwSilverBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_bing_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_bing_silver",
		Bucket:        BiztechEdwSilverBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_bqmig_uat",
		Bucket:        BiztechEdwSilverBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
	},
	{
		Name:          "biztech_edw_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_bronze_productapi_main",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_bronze_s3_inventory",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_ci",
		Bucket:        BiztechEdwCiBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_csops_gold",
		Bucket:        BiztechEdwGoldBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_customer_success_gold",
		Bucket:        BiztechEdwGoldBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataSensitive,
			team.CustomerSuccessOperations,
			team.DecisionScience,
			team.FinancialOperations,
			team.MarketingDataAnalytics,
			team.DataEngineering,
			team.DataTools,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-csops-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-finance":          {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_dataquality_gold",
		Bucket:        BiztechEdwGoldBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "biztech_edw_delighted_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_dev",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-csops-developers":         {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_docebo_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_extracts_gold",
		Bucket:        BiztechEdwGoldBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataSensitive,
			team.DecisionScience,
			team.FinancialOperations,
			team.MarketingDataAnalytics,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-finance": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_finance_strategy_gold",
		Bucket:        BiztechEdwGoldBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_financeops_dev",
		Bucket:        BiztechEdwSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edw_financeops_gold",
		Bucket:        BiztechEdwSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edw_financeops_silver",
		Bucket:        BiztechEdwSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edw_fivetranlog_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_gainsight_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_googleads_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_greenhouse_bronze",
		Bucket:        BiztechEdwSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edw_iterable_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_iterable_silver",
		Bucket:        BiztechEdwSilverBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_jira_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_lessonly_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_marketo_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_marketo_silver",
		Bucket:        BiztechEdwSilverBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_netsuite2_sb1",
		Bucket:        BiztechEdwUatBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_netsuite_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_netsuite_sb1",
		Bucket:        BiztechEdwUatBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_netsuite_sb2",
		Bucket:        BiztechEdwUatBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_netsuite_silver",
		Bucket:        BiztechEdwSilverBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataSensitive,
			team.CustomerSuccessOperations,
			team.DecisionScience,
			team.MarketingDataAnalytics,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-csops-developers":         {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_peopleops_dev",
		Bucket:        BiztechEdwSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edw_peopleops_gold",
		Bucket:        BiztechEdwSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edw_peopleops_silver",
		Bucket:        BiztechEdwSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edw_salescomp_gold",
		Bucket:        BiztechEdwGoldBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edw_salesforce_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_salesforce_silver",
		Bucket:        BiztechEdwSilverBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataSensitive,
			team.BusinessSystems,
			team.CustomerSuccessOperations,
			team.DecisionScience,
			team.Hardware,
			team.MarketingDataAnalytics,
			team.GTMS,
			team.SeOps,
			team.FirmwareCore,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-migration-gtms":         {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-sales-engineering":      {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-csops-developers":       {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_salesforce_uat",
		Bucket:        BiztechEdwUatBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_seops_gold",
		Bucket:        BiztechEdwGoldBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_silver",
		Bucket:        BiztechEdwSilverBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataSensitive,
			team.BusinessSystemsRecruiting,
			team.CustomerSuccessOperations,
			team.DataAnalytics,
			team.DataEngineering,
			team.DecisionScience,
			team.FinancialOperations,
			team.Hardware,
			team.MarketingDataAnalytics,
			team.PlatformOperations,
			team.ProductManagement,
			team.SalesDataAnalytics,
			team.SalesEngineering,
			team.SeOps,
			team.SupplyChain,
			team.Support,
			team.TPM,
			team.ConnectedEquipment,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":     {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-csops-developers":           {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":                {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-finance":                    {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-sales-engineering":          {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-supplychain":                {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-migration-salesengineering": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_sixsense_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_sixsense_silver",
		Bucket:        BiztechEdwSilverBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_sterling",
		Bucket:        BiztechEdwSterlingBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_stripe_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_supply_chain_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_supply_chain_gold",
		Bucket:        BiztechEdwGoldBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_thought_industry_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_ti_silver",
		Bucket:        BiztechEdwSilverBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_uat",
		Bucket:        BiztechEdwUatBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadGroups: []components.TeamInfo{
			team.Accounting,
			team.BizTechEnterpriseData,
			team.DataAnalytics,
			team.DataEngineering,
			team.FinancialOperations,
			team.SalesDataAnalytics,
			team.Support,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataApplication,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":            {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-finance":                {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_workday_bronze",
		Bucket:        BiztechEdwSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edw_xactly_bronze",
		Bucket:        BiztechEdwSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edw_xactly_silver",
		Bucket:        BiztechEdwSilverBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_zendesk_bronze",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edw_zendesk_uat",
		Bucket:        BiztechEdwUatBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataAdmin,
		},
		CanReadWriteBiztechGroups: map[string][]string{
			"bt-bted-dbx-admins": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_edws_adaptive_bronze",
		Bucket:        BiztechEdwSensitiveBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_ci",
		Bucket:        BiztechEdwSensitiveCiBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_finops_gold",
		Bucket:        BiztechEdwFinopsSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_finops_silver",
		Bucket:        BiztechEdwFinopsSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_galtobelli",
		Bucket:        BiztechEdwSensitiveDevBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_greenhouse_bronze",
		Bucket:        BiztechEdwSensitiveBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_peopleops_gold",
		Bucket:        BiztechEdwPeopleopsSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_peopleops_silver",
		Bucket:        BiztechEdwPeopleopsSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_salescomp_gold",
		Bucket:        BiztechEdwSalescompSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_samsarapeople_zendesk_bronze",
		Bucket:        BiztechEdwSalescompSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_sterling",
		Bucket:        BiztechEdwSensitiveBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_uat",
		Bucket:        BiztechEdwSensitiveDevBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_workday_bronze",
		Bucket:        BiztechEdwSensitiveBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_xactly_bronze",
		Bucket:        BiztechEdwSensitiveBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_edws_zendesk_bronze",
		Bucket:        BiztechEdwSensitiveBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "biztech_galtobelli",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_hvenkatesh",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_isingh",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_kheom",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "biztech_klee",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":            {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_kpatro",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_mvaz",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "biztech_ngadiraju",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_ngurram",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_nmendoza",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":            {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_pmuthusamy",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "biztech_rvera",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_sdas",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_spendry",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_tkelley",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "biztech_tlee",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
			MainAWSAccountEU,
			MainAWSAccountCA,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "fivetran_backspace_compatible_staging",
		Bucket:        BiztechEdwSensitiveBucket,
		OwnerTeam:     team.BizTechEnterpriseDataSensitive,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
	},
	{
		Name:          "fivetran_bacterium_rugged_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_bonelike_encourage_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_consoling_inhibition_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_excel_inguinal_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_greenhouse_recruiting",
		Bucket:        FivetranGreenhouseRecruitingDeltaTablesBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		SkipGlueCatalog: true,
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BusinessSystemsRecruiting,
		},
	},
	{
		Name:          "fivetran_longitudinal_aflame_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_monorail_nineteen_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_numbered_undermost_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_petunia_background_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.DataScience,
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_phoenix_logarithmic_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_rural_prepaid_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_shelf_cholera_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_silvery_irresponsible_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BusinessSystems,
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.PlatformOperations,
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_spearman_beside_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BusinessSystems,
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.PlatformOperations,
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "fivetran_viewable_torn_staging",
		Bucket:        BiztechEdwBronzeBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: []components.TeamInfo{
			team.BizTechEnterpriseDataSensitive,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.BizTechEnterpriseData,
			team.BizTechEnterpriseDataApplication,
		},
	},
	{
		Name:          "kpatro_poc_biztech_edw_bronze",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "kpatro_poc_biztech_edw_gold",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
	},
	{
		Name:          "kpatro_poc_biztech_edw_silver",
		Bucket:        BiztechEdwDevBucket,
		OwnerTeam:     team.BizTechEnterpriseData,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadWriteGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadBiztechGroups: map[string][]string{
			"bt-techsupport": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name: "samsara_zendesk",

		Bucket:        ZendeskBucket,
		OwnerTeam:     team.BizTechEnterpriseDataAdmin,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			MainAWSAccountUS,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DataScience,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		},
	},
}
