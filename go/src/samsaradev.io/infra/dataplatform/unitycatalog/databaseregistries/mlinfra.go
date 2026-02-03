package databaseregistries

import (
	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

var MLInfraDatabases = []SamsaraDB{ // lint: +sorted
	{
		Name:          "aiml_predictive_maintenance",
		Bucket:        MlFeatureStoreBucket,
		OwnerTeam:     team.MLInfra,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			infraconsts.SamsaraAWSDataScienceProdAccountID,
		},
	},
	{
		Name:          "aiml_shadow_predictive_maintenance_prediction",
		Bucket:        StagingPredictionStoreShadowBucket,
		OwnerTeam:     team.MLInfra,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			infraconsts.SamsaraAWSDataScienceAccountID,
		},
	},
	{
		Name:          "aiml_staging_predictive_maintenance",
		Bucket:        StagingFeatureStoreBucket,
		OwnerTeam:     team.MLInfra,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			infraconsts.SamsaraAWSDataScienceAccountID,
		},
	},
	{
		Name:          "aiml_staging_predictive_maintenance_prediction",
		Bucket:        StagingPredictionStoreBucket,
		OwnerTeam:     team.MLInfra,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			infraconsts.SamsaraAWSDataScienceAccountID,
		},
	},
	{
		Name:          "dojo",
		Bucket:        AiDatasetsBucket,
		OwnerTeam:     team.MLInfra,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			infraconsts.SamsaraAWSDataScienceAccountID,
			infraconsts.SamsaraAWSEUDataScienceAccountID,
			infraconsts.SamsaraAWSCADataScienceAccountID,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DataScience,
			team.MLCV,
			team.MLScience,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-biztech-api-developers":   {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-gtm-marketing-developers": {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-techsupport":              {infraconsts.SamsaraAWSDefaultRegion, infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
			"bt-support-all-users":        {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "dojo_tmp",
		Bucket:        DojoTemptablesBucket,
		OwnerTeam:     team.MLInfra,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			infraconsts.SamsaraAWSDataScienceAccountID,
			infraconsts.SamsaraAWSEUDataScienceAccountID,
			infraconsts.SamsaraAWSCADataScienceAccountID,
		},
		CanReadGroups: []components.TeamInfo{
			team.SafetyPlatform,
			team.SafetyEventIngestionAndFiltering,
		},
		CanReadWriteGroups: []components.TeamInfo{
			team.DataScience,
			team.MLCV,
		},
		CanReadBiztechGroups: map[string][]string{
			"bt-support-all-users": {infraconsts.SamsaraAWSCARegion},
		},
	},
	{
		Name:          "labelbox",
		Bucket:        LabelboxBucket,
		OwnerTeam:     team.MLInfra,
		DatabaseGroup: CustomBucketGeneralDatabaseGroup,
		AWSAccountIDs: []string{
			infraconsts.SamsaraAWSDataScienceAccountID,
			infraconsts.SamsaraAWSEUDataScienceAccountID,
			infraconsts.SamsaraAWSCADataScienceAccountID,
		},
		CanReadGroups: clusterhelpers.DatabricksClusterTeams(),
		CanReadWriteGroups: []components.TeamInfo{
			team.DataScience,
			team.MLCV,
		},
	},
}
