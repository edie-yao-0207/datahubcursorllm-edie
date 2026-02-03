package unitycatalog

import (
	"samsaradev.io/infra/app/generate_terraform/databricksresource_official"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformprojects/emrreplicationproject"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
)

type StorageLocation struct {
	UnprefixedBucketName string
	DisallowedRegions    []string

	// The IAM role we make for these resources needs to be self-assuming.
	// Unfortunately this makes it extremely hard to make via terraform.
	// We set "NeedsCreation_DONOTUSE" as true initially, and after its created we
	// can set it as false, so the self-assuming portion of the role can be added
	// successfully.
	NeedsCreation_DONOTUSE bool
	// A flag to indicate if the storage location can be created with self-assuming
	// role and policy in one step.
	SelfAssumingRole bool
	// IAM role ARN for buckets manages outside of backend repo can be provided here.
	ExternalIAMRoleArn string
	// This can be used to set owner principal of external location. This should
	// be set only for biztech locations.
	OwnerGroup string
	// This can be used to set read only flag for external location.
	ReadOnly bool

	// Map of user/group principal to their granted privileges
	Privileges map[string][]databricksresource_official.GrantPrivilege
}

func AllStorageLocations(config dataplatformconfig.DatabricksConfig) []StorageLocation {
	for i := range newStorageLocations {
		newStorageLocations[i].SelfAssumingRole = true
	}
	allStorageLocations := append(legacyStorageLocations_DONOTADD, newStorageLocations...)
	allStorageLocations = append(allStorageLocations, getEmrReplicationStorageLocations(config)...)
	return allStorageLocations
}

// Function to check if a bucket exists in a given region.
func BucketExistsInRegion(bucketName string, config dataplatformconfig.DatabricksConfig) bool {
	region := config.Region
	for _, location := range AllStorageLocations(config) {
		if location.UnprefixedBucketName == bucketName {
			// Check if the region is not disallowed for this bucket.
			for _, disallowedRegion := range location.DisallowedRegions {
				if disallowedRegion == region {
					return false // The region is disallowed.
				}
			}
			return true // The bucket exists and the region is allowed.
		}
	}
	return false // No bucket found with the given name.
}

func getEmrReplicationStorageLocations(config dataplatformconfig.DatabricksConfig) []StorageLocation {
	emrReplicationExportBucketNames := emrreplicationproject.GetAllEmrReplicationExportBucketNames(config)
	emrReplicationDeltaLakeBucketNames := emrreplicationproject.GetAllEmrReplicationDeltaLakeBucketNames(config)

	emrReplicationStorageLocations := []StorageLocation{}
	for _, bucketName := range emrReplicationExportBucketNames {
		emrReplicationStorageLocations = append(emrReplicationStorageLocations, StorageLocation{
			UnprefixedBucketName: bucketName,
			SelfAssumingRole:     true,
		})
	}
	for _, bucketName := range emrReplicationDeltaLakeBucketNames {
		emrReplicationStorageLocations = append(emrReplicationStorageLocations, StorageLocation{
			UnprefixedBucketName: bucketName,
			SelfAssumingRole:     true,
		})
	}
	return emrReplicationStorageLocations
}

// Do not add new storage locations here. Add them to newStorageLocations.
// This is a list of legacy storage locations that required a 2-step process to create
// the self-assuming role and policy. This is now done in one step for new storage locations.
var legacyStorageLocations_DONOTADD = []StorageLocation{
	// biztech storage locations with biztechEnterpriseDataAdmin as owner.
	{
		UnprefixedBucketName: databaseregistries.BtedEdwCatDev,
		ExternalIAMRoleArn:   databaseregistries.BtedExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BtedEdwCatSensitiveDev,
		ExternalIAMRoleArn:   databaseregistries.BtedExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BtedEdwCatUat,
		ExternalIAMRoleArn:   databaseregistries.BtedExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BtedEdwCatSensitiveUat,
		ExternalIAMRoleArn:   databaseregistries.BtedExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BtedEdwCatBronze,
		ExternalIAMRoleArn:   databaseregistries.BtedExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BtedEdwCatSensitiveBronze,
		ExternalIAMRoleArn:   databaseregistries.BtedExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BtedEdwCatMain,
		ExternalIAMRoleArn:   databaseregistries.BtedExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BtedEdwCatSensitiveMain,
		ExternalIAMRoleArn:   databaseregistries.BtedExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BtedEdwCatBusinessdbs,
		ExternalIAMRoleArn:   databaseregistries.BtedExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BtedEdwCatSensitiveBusinessdbs,
		ExternalIAMRoleArn:   databaseregistries.BtedExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BtedImports,
		ExternalIAMRoleArn:   databaseregistries.BtedExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwBronzeBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwCiBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwDevBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwFinopsSensitiveBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwGoldBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwPeopleopsSensitiveBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwSalescompSensitiveBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwSensitiveBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwSensitiveBronzeBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwSensitiveCiBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwSensitiveDevBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwSilverBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwSterlingBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechEdwUatBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
	},
	{
		UnprefixedBucketName: databaseregistries.BiztechSdaProductionBucket,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	// miscellaneous storage locations with CI user as owner by default.
	{
		UnprefixedBucketName: databaseregistries.KinesisstatsDeltaLakeBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.RdsDeltaLakeBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DynamoDbDeltaLakeBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.S3BigStatsDeltaLakeBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DataPipelinesDeltaLakeBucket,
		Privileges: map[string][]databricksresource_official.GrantPrivilege{
			"datapipelines-lambda": {
				databricksresource_official.GrantStorageCredentialReadFiles,
				databricksresource_official.GrantStorageCredentialWriteFiles,
				databricksresource_official.GrantStorageCredentialCreateExternalTable,
			},
			"datapipelines-lambda-service-principal": {
				databricksresource_official.GrantStorageCredentialReadFiles,
				databricksresource_official.GrantStorageCredentialWriteFiles,
				databricksresource_official.GrantStorageCredentialCreateExternalTable,
			},
		},
	},
	{
		UnprefixedBucketName: databaseregistries.DatastreamLakeBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DatastreamsDeltaLakeBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.ProdAppConfigsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DataplatformDeployedArtifactsBucket,
		Privileges: map[string][]databricksresource_official.GrantPrivilege{
			"datapipelines-lambda":                   {databricksresource_official.GrantStorageCredentialReadFiles},
			"datapipelines-lambda-service-principal": {databricksresource_official.GrantStorageCredentialReadFiles},
			"metrics-ci-user":                        {databricksresource_official.GrantStorageCredentialReadFiles},
			"metrics-service-principal":              {databricksresource_official.GrantStorageCredentialReadFiles},
		},
	},
	{
		UnprefixedBucketName: databaseregistries.ReportStagingTablesBucket,
		Privileges: map[string][]databricksresource_official.GrantPrivilege{
			"datapipelines-lambda": {
				databricksresource_official.GrantStorageCredentialReadFiles,
				databricksresource_official.GrantStorageCredentialWriteFiles,
				databricksresource_official.GrantStorageCredentialCreateExternalTable,
			},
			"datapipelines-lambda-service-principal": {
				databricksresource_official.GrantStorageCredentialReadFiles,
				databricksresource_official.GrantStorageCredentialWriteFiles,
				databricksresource_official.GrantStorageCredentialCreateExternalTable,
			},
		},
	},
	{
		UnprefixedBucketName: databaseregistries.S3InventoryBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.AiDatasetsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.LegacyDatabricksPlaygroundBucket_DONOTUSE,
	},
	{
		UnprefixedBucketName: databaseregistries.DatabricksWarehouseBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DatabricksWorkspaceBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DatamodelWarehouseBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DatamodelWarehouseDevBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DojoTemptablesBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.EpofinanceProdBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.FivetranGreenhouseRecruitingDeltaTablesBucket,
		OwnerGroup:           team.BusinessSystemsRecruiting.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.FivetranIncidentioDeltaTablesBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.LabelboxBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.MarketingDataAnalyticsBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.MlFeatureStoreBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.NetsuiteFinanceBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.SafetyMapDataSourcesBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.StagingFeatureStoreBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.StagingPredictionStoreBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.StagingPredictionStoreShadowBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.SupplychainBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.ZendeskBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.MixpanelBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.DataPipelinesDeltaLakeFromEuWest1Bucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.DataPlatformFivetranBronzeBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.FivetranNetsuiteFinanceDeltaTablesBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.FivetranSamsaraSourceQaBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.DatabricksS3InventoryBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.NetsuiteDeltaTablesBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.ProdAppLogsBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.DatabricksBillingBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.DatabricksCloudtrailBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.ExampleCustomerWarehouseBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.DataEngMappingTablesBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.SamsaraCvDataBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.SamsaraDashcamVideosBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.SamsaraMlModelsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.SamsaraWorkforceVideoAssetsBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.SensorReplayBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.FirmwareTestAutomationBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.LoadBalancerAccesLogsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DatabricksKinesisstatsDiffsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.VodafoneReportsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.EC2LogsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.AWSReportsBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.OrgWideStorageLensExportBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.PreferencesProductionBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.PreferencesSandboxBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.NetsuiteSamReportsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DatabricksNetsuiteInvoiceOutputBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.PlatopsDatabricksMetadataBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.NetsuiteInvoiceExportsBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.YearReviewMetricsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DataInsightsBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.DataScienceExploratoryBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.DatabricksSqlQueryHistoryBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.BenchmarkingMetricsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.AttSftpBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.PartialReportAggregationBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DevmetricsBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.ThorVariantsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DatasetsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.WorkforceDataBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.BigdataDatashareBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.DojoDataExchangeBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.GtmsBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.DetailedIftaReportsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.EpofinanceBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.NetsuiteLicensesBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.FivetranSamsaraConnectorDemoBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.NetworkCoverageBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.SupportCloudwatchLogsBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
	{
		UnprefixedBucketName: databaseregistries.MapTilesBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.SmartMapsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DatabricksAuditLogBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion, infraconsts.SamsaraAWSEURegion},
	},
}

var newStorageLocations = []StorageLocation{
	{
		UnprefixedBucketName: databaseregistries.DatabricksDnsResolverQueryLogsBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.MarketingScoreLeadsSandbox,
		ExternalIAMRoleArn:   databaseregistries.MarketingExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.MarketingScoreLeadsProduction,
		ExternalIAMRoleArn:   databaseregistries.MarketingExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.MapsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.MapsPlacesMigrationBucket,
		OwnerGroup:           team.Maps.DatabricksAccountGroupName(),
	},
	{
		UnprefixedBucketName: databaseregistries.FivetranPagerDutyDeltaTablesBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.GqlMappingBucket,
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.DynamoDbExportBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.RdsExportBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.RdsKinesisExportBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.KinesisstatsJsonExportBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.S3BigStatsJsonExportBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.ApiLoadBalancerAccesLogsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.CommercialNavigationDailyDriverSessionCountBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DataHubMetadataBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.QueryAgentsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DssMlModelsBucket,
	},

	{
		UnprefixedBucketName: databaseregistries.DeviceInstallationMediaUploadsBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.CSVPostProcessedBucket,
	},
	{
		UnprefixedBucketName: databaseregistries.DojoSamsara365KBucket,
		OwnerGroup:           team.MLInfra.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
	},
	{
		UnprefixedBucketName: databaseregistries.ProductAPIInvoicesBucket,
		ExternalIAMRoleArn:   databaseregistries.BtedExternalIAMRoleArn,
		OwnerGroup:           team.BizTechEnterpriseDataAdmin.DatabricksAccountGroupName(),
		DisallowedRegions:    []string{infraconsts.SamsaraAWSEURegion, infraconsts.SamsaraAWSCARegion},
		ReadOnly:             true,
	},
}
