package unitycatalog

import (
	"testing"

	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/libs/ni/infraconsts"
)

func TestVolumeSchemaStorageLocation(t *testing.T) {
	// Sample test data
	volumeSchemas := S3BucketVolumeSchemas

	storageLocations := AllStorageLocations(dataplatformconfig.DatabricksConfig{
		Region: infraconsts.SamsaraAWSDefaultRegion,
	})

	// Map storage locations for quick lookup
	storageLocationMap := make(map[string]struct{})
	for _, sl := range storageLocations {
		storageLocationMap[sl.UnprefixedBucketName] = struct{}{}
	}

	// Check each volume schema
	for _, vs := range volumeSchemas {
		if _, exists := storageLocationMap[vs.UnprefixedBucketLocation]; !exists {
			t.Errorf("UnprefixedBucketLocation %s does not have a matching StorageLocation", vs.UnprefixedBucketLocation)
		}
	}
}

func TestNoNewLegacyStorageLocations(t *testing.T) {
	legacyStorageLocationsMap := map[string]struct{}{
		databaseregistries.BtedEdwCatDev:                                 {},
		databaseregistries.BtedEdwCatSensitiveDev:                        {},
		databaseregistries.BtedEdwCatUat:                                 {},
		databaseregistries.BtedEdwCatSensitiveUat:                        {},
		databaseregistries.BtedEdwCatBronze:                              {},
		databaseregistries.BtedEdwCatSensitiveBronze:                     {},
		databaseregistries.BtedEdwCatMain:                                {},
		databaseregistries.BtedEdwCatSensitiveMain:                       {},
		databaseregistries.BtedEdwCatBusinessdbs:                         {},
		databaseregistries.BtedEdwCatSensitiveBusinessdbs:                {},
		databaseregistries.BtedImports:                                   {},
		databaseregistries.BiztechEdwBronzeBucket:                        {},
		databaseregistries.BiztechEdwCiBucket:                            {},
		databaseregistries.BiztechEdwDevBucket:                           {},
		databaseregistries.BiztechEdwFinopsSensitiveBucket:               {},
		databaseregistries.BiztechEdwGoldBucket:                          {},
		databaseregistries.BiztechEdwPeopleopsSensitiveBucket:            {},
		databaseregistries.BiztechEdwSalescompSensitiveBucket:            {},
		databaseregistries.BiztechEdwSensitiveBucket:                     {},
		databaseregistries.BiztechEdwSensitiveBronzeBucket:               {},
		databaseregistries.BiztechEdwSensitiveCiBucket:                   {},
		databaseregistries.BiztechEdwSensitiveDevBucket:                  {},
		databaseregistries.BiztechEdwSilverBucket:                        {},
		databaseregistries.BiztechEdwSterlingBucket:                      {},
		databaseregistries.BiztechEdwUatBucket:                           {},
		databaseregistries.BiztechSdaProductionBucket:                    {},
		databaseregistries.KinesisstatsDeltaLakeBucket:                   {},
		databaseregistries.RdsDeltaLakeBucket:                            {},
		databaseregistries.DynamoDbDeltaLakeBucket:                       {},
		databaseregistries.S3BigStatsDeltaLakeBucket:                     {},
		databaseregistries.DataPipelinesDeltaLakeBucket:                  {},
		databaseregistries.DatastreamLakeBucket:                          {},
		databaseregistries.DatastreamsDeltaLakeBucket:                    {},
		databaseregistries.ProdAppConfigsBucket:                          {},
		databaseregistries.DataplatformDeployedArtifactsBucket:           {},
		databaseregistries.ReportStagingTablesBucket:                     {},
		databaseregistries.S3InventoryBucket:                             {},
		databaseregistries.AiDatasetsBucket:                              {},
		databaseregistries.LegacyDatabricksPlaygroundBucket_DONOTUSE:     {},
		databaseregistries.DatabricksWarehouseBucket:                     {},
		databaseregistries.DatabricksWorkspaceBucket:                     {},
		databaseregistries.DatamodelWarehouseBucket:                      {},
		databaseregistries.DatamodelWarehouseDevBucket:                   {},
		databaseregistries.DojoTemptablesBucket:                          {},
		databaseregistries.EpofinanceProdBucket:                          {},
		databaseregistries.FivetranGreenhouseRecruitingDeltaTablesBucket: {},
		databaseregistries.FivetranIncidentioDeltaTablesBucket:           {},
		databaseregistries.LabelboxBucket:                                {},
		databaseregistries.MarketingDataAnalyticsBucket:                  {},
		databaseregistries.MlFeatureStoreBucket:                          {},
		databaseregistries.NetsuiteFinanceBucket:                         {},
		databaseregistries.SafetyMapDataSourcesBucket:                    {},
		databaseregistries.StagingFeatureStoreBucket:                     {},
		databaseregistries.StagingPredictionStoreBucket:                  {},
		databaseregistries.StagingPredictionStoreShadowBucket:            {},
		databaseregistries.SupplychainBucket:                             {},
		databaseregistries.ZendeskBucket:                                 {},
		databaseregistries.MixpanelBucket:                                {},
		databaseregistries.DataPipelinesDeltaLakeFromEuWest1Bucket:       {},
		databaseregistries.DataPlatformFivetranBronzeBucket:              {},
		databaseregistries.FivetranNetsuiteFinanceDeltaTablesBucket:      {},
		databaseregistries.FivetranSamsaraSourceQaBucket:                 {},
		databaseregistries.DatabricksS3InventoryBucket:                   {},
		databaseregistries.NetsuiteDeltaTablesBucket:                     {},
		databaseregistries.ProdAppLogsBucket:                             {},
		databaseregistries.DatabricksBillingBucket:                       {},
		databaseregistries.DatabricksCloudtrailBucket:                    {},
		databaseregistries.ExampleCustomerWarehouseBucket:                {},
		databaseregistries.DataEngMappingTablesBucket:                    {},
		databaseregistries.SamsaraCvDataBucket:                           {},
		databaseregistries.SamsaraDashcamVideosBucket:                    {},
		databaseregistries.SamsaraMlModelsBucket:                         {},
		databaseregistries.SamsaraWorkforceVideoAssetsBucket:             {},
		databaseregistries.SensorReplayBucket:                            {},
		databaseregistries.FirmwareTestAutomationBucket:                  {},
		databaseregistries.LoadBalancerAccesLogsBucket:                   {},
		databaseregistries.DatabricksKinesisstatsDiffsBucket:             {},
		databaseregistries.VodafoneReportsBucket:                         {},
		databaseregistries.EC2LogsBucket:                                 {},
		databaseregistries.AWSReportsBucket:                              {},
		databaseregistries.OrgWideStorageLensExportBucket:                {},
		databaseregistries.PreferencesProductionBucket:                   {},
		databaseregistries.PreferencesSandboxBucket:                      {},
		databaseregistries.NetsuiteSamReportsBucket:                      {},
		databaseregistries.DatabricksNetsuiteInvoiceOutputBucket:         {},
		databaseregistries.PlatopsDatabricksMetadataBucket:               {},
		databaseregistries.NetsuiteInvoiceExportsBucket:                  {},
		databaseregistries.YearReviewMetricsBucket:                       {},
		databaseregistries.DataInsightsBucket:                            {},
		databaseregistries.DataScienceExploratoryBucket:                  {},
		databaseregistries.DatabricksSqlQueryHistoryBucket:               {},
		databaseregistries.BenchmarkingMetricsBucket:                     {},
		databaseregistries.AttSftpBucket:                                 {},
		databaseregistries.PartialReportAggregationBucket:                {},
		databaseregistries.DevmetricsBucket:                              {},
		databaseregistries.ThorVariantsBucket:                            {},
		databaseregistries.DatasetsBucket:                                {},
		databaseregistries.WorkforceDataBucket:                           {},
		databaseregistries.BigdataDatashareBucket:                        {},
		databaseregistries.DojoDataExchangeBucket:                        {},
		databaseregistries.GtmsBucket:                                    {},
		databaseregistries.DetailedIftaReportsBucket:                     {},
		databaseregistries.EpofinanceBucket:                              {},
		databaseregistries.NetsuiteLicensesBucket:                        {},
		databaseregistries.FivetranSamsaraConnectorDemoBucket:            {},
		databaseregistries.NetworkCoverageBucket:                         {},
		databaseregistries.SupportCloudwatchLogsBucket:                   {},
		databaseregistries.MapTilesBucket:                                {},
		databaseregistries.SmartMapsBucket:                               {},
		databaseregistries.DatabricksAuditLogBucket:                      {},
	}

	// Map storage locations for quick lookup
	for _, sl := range legacyStorageLocations_DONOTADD {
		if _, exists := legacyStorageLocationsMap[sl.UnprefixedBucketName]; !exists {
			t.Errorf("UnprefixedBucketLocation %s is added to legacyStorageLocations_DONOTADD list. Please add it to the newStorageLocations list", sl.UnprefixedBucketName)
		}
	}

}
