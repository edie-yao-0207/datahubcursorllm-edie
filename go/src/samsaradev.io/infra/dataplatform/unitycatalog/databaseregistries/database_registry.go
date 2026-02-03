package databaseregistries

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/infra/dataplatform/rdsdeltalake"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

type DatabaseGroup string

const (
	// Production databases are located in databricks-warehouse bucket.
	ProductionDatabaseGroup DatabaseGroup = "PRODUCTION"

	// Team Dev Databases for testing live in the "databricks-workspace" bucket.
	// These will eventually have a retention job which will periodically remove any old tables.
	TeamDevDatabaseGroup DatabaseGroup = "TEAM_DEV"

	// Fivetran databases should only exist in the US region because each
	// fivetran destination only syncs to a single location which is a
	// cluster in the US region. If fivetran data is needed in the EU, a
	// cross region replication can be setup on the destination bucket.
	FivetranDatabaseGroup DatabaseGroup = "FIVETRAN"

	// Databases in the "databricks-playground" bucket. Only for legacy DBs. Do not add any more databases.
	PlaygroundDatabaseGroup_LEGACY DatabaseGroup = "PLAYGROUND"

	// Databases in custom S3 buckets.
	CustomBucketGeneralDatabaseGroup DatabaseGroup = "CUSTOM_BUCKET_GENERAL"
	// Data model DBs live in the "datamodel-warehouse" bucket.
	CustomBucketDataModelProductionNonGADatabaseGroup  DatabaseGroup = "CUSTOM_BUCKET_DATA_MODEL_PRODUCTION_NON_GA"
	CustomBucketDataModelDevelopmentNonGADatabaseGroup DatabaseGroup = "CUSTOM_BUCKET_DATA_MODEL_DEVELOPMENT_NON_GA"
	// Data model databases for general audience.
	CustomBucketDataModelProductionGADatabaseGroup  DatabaseGroup = "CUSTOM_BUCKET_DATA_MODEL_PRODUCTION_GA"
	CustomBucketDataModelDevelopmentGADatabaseGroup DatabaseGroup = "CUSTOM_BUCKET_DATA_MODEL_DEVELOPMENT_GA"

	RdsDatabaseGroup DatabaseGroup = "RDS"
	// Individual shards backing the RDS combined shards schemas.
	RdsShardsDatabaseGroup DatabaseGroup = "RDS_SHARDS"

	// EMR databases
	EmrDatabaseGroup DatabaseGroup = "EMR"
)

// S3 Buckets.
const (
	DatabricksWorkspaceBucket             = "databricks-workspace"
	DatabricksWarehouseBucket             = "databricks-warehouse"
	DatamodelWarehouseBucket              = "datamodel-warehouse"
	DatamodelWarehouseDevBucket           = "datamodel-warehouse-dev"
	DatabricksRdsDiffsBucket              = "databricks-rds-diffs"
	RdsExportBucket                       = "rds-export"
	RdsKinesisExportBucket                = "rds-kinesis-export"
	DynamoDbExportBucket                  = "dynamodb-export"
	FivetranIncidentioDeltaTablesBucket   = "fivetran-incidentio-delta-tables"
	FivetranLaunchdarklyDeltaTablesBucket = "fivetran-launchdarkly-delta-tables"
	FivetranPagerDutyDeltaTablesBucket    = "fivetran-pagerduty-delta-tables"
	FivetranSamsaraSourceQaBucket         = "fivetran-samsara-source-qa"
	FivetranSamsaraConnectorDemoBucket    = "fivetran-samsara-connector-demo"
	DataPlatformFivetranBronzeBucket      = "dataplatform-fivetran-bronze"
	NetsuiteFinanceBucket                 = "netsuite-finance"
	S3InventoryBucket                     = "s3-inventory"
	DatabricksS3InventoryBucket           = "databricks-s3-inventory"
	// Do not add any more databases to databricks playground bucket.
	LegacyDatabricksPlaygroundBucket_DONOTUSE = "databricks-playground"
	SafetyMapDataSourcesBucket                = "safety-map-data-sources"
	MapTilesBucket                            = "map-tiles"
	SupplychainBucket                         = "supplychain"
	EpofinanceProdBucket                      = "epofinance-prod"

	// Platform Admin Buckets
	CSVPostProcessedBucket = "csv-postprocessed"

	// Data Plat Buckets for Replication into Delta Lake.
	DataPipelinesDeltaLakeBucket            = "data-pipelines-delta-lake"
	DataPipelinesDeltaLakeFromEuWest1Bucket = "data-pipelines-delta-lake-from-eu-west-1"
	KinesisstatsDeltaLakeBucket             = "kinesisstats-delta-lake"
	KinesisstatsJsonExportBucket            = "kinesisstats-json-export"
	S3BigStatsDeltaLakeBucket               = "s3bigstats-delta-lake"
	S3BigStatsJsonExportBucket              = "s3bigstats-json-export"
	RdsDeltaLakeBucket                      = "rds-delta-lake"
	DatastreamsDeltaLakeBucket              = "data-streams-delta-lake"
	DatastreamLakeBucket                    = "data-stream-lake"
	DynamoDbDeltaLakeBucket                 = "dynamodb-delta-lake"
	ReportStagingTablesBucket               = "report-staging-tables"

	// Biztech Buckets.
	BiztechEdwDevBucket                           = "biztech-edw-dev"
	BiztechEdwGoldBucket                          = "biztech-edw-gold"
	BiztechEdwSilverBucket                        = "biztech-edw-silver"
	BiztechEdwBronzeBucket                        = "biztech-edw-bronze"
	BiztechEdwUatBucket                           = "biztech-edw-uat"
	BiztechEdwCiBucket                            = "biztech-edw-ci"
	BiztechEdwSterlingBucket                      = "biztech-edw-sterling"
	BiztechEdwSensitiveDevBucket                  = "biztech-edw-sensitive-dev"
	BiztechEdwSensitiveBronzeBucket               = "biztech-edw-sensitive-bronze"
	ZendeskBucket                                 = "zendesk"
	BiztechSdaProductionBucket                    = "biztech-sda-production"
	BiztechEdwPeopleopsSensitiveBucket            = "biztech-edw-peopleops-sensitive"
	BiztechEdwSensitiveBucket                     = "biztech-edw-sensitive"
	BiztechEdwSensitiveCiBucket                   = "biztech-edw-sensitive-ci"
	BiztechEdwFinopsSensitiveBucket               = "biztech-edw-finops-sensitive"
	BiztechEdwSalescompSensitiveBucket            = "biztech-edw-salescomp-sensitive"
	MarketingDataAnalyticsBucket                  = "marketing-data-analytics"
	FivetranGreenhouseRecruitingDeltaTablesBucket = "fivetran-greenhouse-recruiting-delta-tables"
	FivetranNetsuiteFinanceDeltaTablesBucket      = "fivetran-netsuite-finance-delta-tables"

	// Bted Buckets in Biztech AWS account.
	BtedEdwCatDev                  = "edw-cat-dev"
	BtedEdwCatSensitiveDev         = "edw-cat-sensitive-dev"
	BtedEdwCatUat                  = "edw-cat-uat"
	BtedEdwCatSensitiveUat         = "edw-cat-sensitive-uat"
	BtedEdwCatBronze               = "edw-cat-bronze"
	BtedEdwCatSensitiveBronze      = "edw-cat-sensitive-bronze"
	BtedEdwCatMain                 = "edw-cat-main"
	BtedEdwCatSensitiveMain        = "edw-cat-sensitive-main"
	BtedEdwCatBusinessdbs          = "edw-cat-businessdbs"
	BtedEdwCatSensitiveBusinessdbs = "edw-cat-sensitive-businessdbs"
	BtedImports                    = "bted-imports"

	// Iam role in Biztech AWS account to be used from storage credentials
	BtedExternalIAMRoleArn = "arn:aws:iam::849340316486:role/bted-dbx-s3-access-role"

	// marketing Buckets in Biztech AWS account.
	MarketingScoreLeadsSandbox    = "score-leads-sandbox"
	MarketingScoreLeadsProduction = "score-leads-production"

	// Iam role in marketing corpweb AWS account to be used from storage credentials
	MarketingExternalIAMRoleArn = "arn:aws:iam::448870025893:role/Databricks-Role"

	// ML Buckets.
	StagingFeatureStoreBucket          = "staging-feature-store"
	StagingPredictionStoreShadowBucket = "staging-prediction-store-shadow"
	StagingPredictionStoreBucket       = "staging-prediction-store"
	MlFeatureStoreBucket               = "ml-feature-store"
	AiDatasetsBucket                   = "ai-datasets"
	LabelboxBucket                     = "labelbox"
	DojoTemptablesBucket               = "dojo-temptables"

	// Sensitive Buckets
	SamsaraCvDataBucket               = "cvdata"
	SamsaraDashcamVideosBucket        = "dashcam-videos"
	SamsaraMlModelsBucket             = "ml-models"
	SamsaraWorkforceVideoAssetsBucket = "workforce-video-assets"

	// Misc Buckets
	SensorReplayBucket                                = "sensor-replay"
	FirmwareTestAutomationBucket                      = "firmware-test-automation"
	LoadBalancerAccesLogsBucket                       = "load-balancer-access-logs"
	ApiLoadBalancerAccesLogsBucket                    = "api-load-balancer-access-logs"
	DatabricksKinesisstatsDiffsBucket                 = "databricks-kinesisstats-diffs"
	ProdAppLogsBucket                                 = "prod-app-logs"
	VodafoneReportsBucket                             = "vodafone-reports"
	EC2LogsBucket                                     = "ec2-logs"
	AWSReportsBucket                                  = "aws-reports"
	OrgWideStorageLensExportBucket                    = "org-wide-storage-lens-export"
	PreferencesProductionBucket                       = "preferences-production"
	PreferencesSandboxBucket                          = "preferences-sandbox"
	NetsuiteSamReportsBucket                          = "netsuite-sam-reports"
	DatabricksNetsuiteInvoiceOutputBucket             = "databricks-netsuite-invoice-output"
	PlatopsDatabricksMetadataBucket                   = "platops-databricks-metadata"
	NetsuiteInvoiceExportsBucket                      = "netsuite-invoice-exports"
	YearReviewMetricsBucket                           = "year-review-metrics"
	DataInsightsBucket                                = "data-insights"
	DataScienceExploratoryBucket                      = "data-science-exploratory"
	DatabricksSqlQueryHistoryBucket                   = "databricks-sql-query-history"
	BenchmarkingMetricsBucket                         = "benchmarking-metrics"
	DssMlModelsBucket                                 = "dss-ml-models"
	DataHubMetadataBucket                             = "datahub-metadata"
	MixpanelBucket                                    = "mixpanel"
	AttSftpBucket                                     = "att-sftp"
	PartialReportAggregationBucket                    = "partial-report-aggregation"
	DevmetricsBucket                                  = "devmetrics"
	ThorVariantsBucket                                = "thor-variants"
	DatasetsBucket                                    = "datasets"
	WorkforceDataBucket                               = "workforce-data"
	BigdataDatashareBucket                            = "bigdata-datashare"
	DojoDataExchangeBucket                            = "dojo-data-exchange"
	GtmsBucket                                        = "gtms"
	DetailedIftaReportsBucket                         = "detailed-ifta-reports"
	EpofinanceBucket                                  = "epofinance"
	NetsuiteLicensesBucket                            = "netsuite-licenses"
	NetworkCoverageBucket                             = "network-coverage"
	SmartMapsBucket                                   = "smart-maps"
	DatabricksAuditLogBucket                          = "databricks-audit-log"
	DatabricksDnsResolverQueryLogsBucket              = "databricks-dns-resolver-query-logs"
	NetsuiteDeltaTablesBucket                         = "netsuite-delta-tables"
	DatabricksBillingBucket                           = "databricks-billing"
	DatabricksCloudtrailBucket                        = "databricks-cloudtrail"
	ExampleCustomerWarehouseBucket                    = "example-customer-warehouse"
	DataEngMappingTablesBucket                        = "data-eng-mapping-tables"
	ProdAppConfigsBucket                              = "prod-app-configs"
	DataplatformDeployedArtifactsBucket               = "dataplatform-deployed-artifacts"
	MapsBucket                                        = "maps"
	MapsPlacesMigrationBucket                         = "maps-places-migration"
	CommercialNavigationDailyDriverSessionCountBucket = "commercial-navigation-daily-driver-session-count"
	JasperRatePlanOptimizationBucket                  = "jasper-rate-plan-optimization"
	AwsbilluploaderBucket                             = "awsbilluploader"
	GqlMappingBucket                                  = "gql-mapping"
	MediaExperienceProdBucket                         = "mediaexperience_prod"
	SparkPlaygroundBucket                             = "spark-playground"
	AmundsenMetadataBucket                            = "amundsen-metadata"
	DeviceInstallationMediaUploadsBucket              = "device-installation-media-uploads"
	DojoSamsara365KBucket                             = "dojo-365k"
	QueryAgentsBucket                                 = "query-agents"
	// Support Buckets
	SupportCloudwatchLogsBucket = "support-cloudwatch-logs-bucket"

	// Product API Buckets
	ProductAPIInvoicesBucket = "productapi-invoices"
)

var MainAWSAccountUS = strconv.Itoa(infraconsts.SamsaraAWSAccountID)
var MainAWSAccountEU = strconv.Itoa(infraconsts.SamsaraAWSEUAccountID)
var MainAWSAccountCA = strconv.Itoa(infraconsts.SamsaraAWSCAAccountID)
var DatabricksAWSAccountUS = strconv.Itoa(infraconsts.SamsaraAWSDatabricksAccountID)
var DatabricksAWSAccountEU = strconv.Itoa(infraconsts.SamsaraAWSEUDatabricksAccountID)
var DatabricksAWSAccountCA = strconv.Itoa(infraconsts.SamsaraAWSCADatabricksAccountID)

type SamsaraDB struct {
	Name string

	// OwnerTeam requires a TeamInfo object to specify the team that owns the database.
	OwnerTeam components.TeamInfo

	// OwnerTeamName is the name of the team that owns the database. This field exists
	// so that we can reference owner teams that were not created within the backend repository.
	// e.g. biztech groups such as 'bt-mda-analytics'
	OwnerTeamName string

	// Bucket is the S3 bucket that contains the database.
	Bucket string

	// The Glue databases have entry points in databricks-warehouse, even though the tables live in a different bucket.
	// We are maintaining this behavior for legacy databases in Glue. For all databases in Unity Catalog, we should use the above "Bucket" field.
	LegacyGlueS3Bucket string

	// Specify the IDs for each AWS account the database exists in. This helps track which AWS account the database is in,
	// and which regions it is available in.
	AWSAccountIDs []string

	// DatabaseGroup is the group that the database belongs to.
	DatabaseGroup DatabaseGroup

	// If true, the database is only generated for Unity Catalog
	// These are for DBs that already exist in the Glue Catalog (Managed or Unmanaged)
	// that we don't want to recreate in Glue but want managed in Unity Catalog.
	// This flag is for Data Platform internal use only.
	SkipGlueCatalog bool

	// List of Databricks group names which have READ access to the database.
	CanReadGroups []components.TeamInfo

	// List of Databricks group names which have READ and WRITE access to the database.
	CanReadWriteGroups []components.TeamInfo

	// CanReadBiztechGroups is a list of biztech groups that can read the table in the default catalog
	// These groups are not managed by dataplatform like R&D teams in CanReadGroups, but are managed by the biztech team
	// and represent Non-R&D teams that use the biztech workspace.
	// The key is the group name and the value is the list of regions the group has access to.
	CanReadBiztechGroups map[string][]string

	// CanReadWriteBiztechGroups is a list of biztech groups that can read and write the table in the default catalog
	// These groups are not managed by dataplatform like R&D teams in CanReadWriteGroups, but are managed by the biztech team
	// and represent Non-R&D teams that use the biztech workspace.
	// The key is the group name and the value is the list of regions the group has access to.
	CanReadWriteBiztechGroups map[string][]string
}

func GetAllTeamDatabases(config dataplatformconfig.DatabricksConfig) [][]SamsaraDB {
	return [][]SamsaraDB{ // lint: +sorted
		AppTelematicsDatabases,
		AssetFoundationsDatabases,
		BizTechEnterpriseDataDatabases,
		BusinessSystemsDatabases,
		BusinessSystemsRecruitingDatabases,
		CloudDatabases,
		ComplianceDatabases,
		ConnectedEquipmentDatabases,
		ConnectedWorkerDatabases,
		CoreServicesDatabases,
		CustomerSuccessOperationsDatabases,
		DataAnalyticsDatabases,
		DataEngineeringDatabases,
		DataPlatformDatabases(config),
		DataScienceDatabases,
		DevEcosystemDatabases,
		DeveloperExperienceDatabases,
		DeviceServicesDatabases,
		DiagnosticToolsDatabases,
		DriverManagementDatabases,
		EpoFinanceDatabases,
		FinancialOperationsDatabases,
		FirmwareDatabases,
		FleetSecDatabases,
		GrowthDatabases,
		GTMSDatabases,
		HardwareDatabases,
		IftaCoreDatabases,
		IngestionDatabases,
		MapsDatabases,
		MarketingDataAnalyticsDatabases,
		MdmSdkDatabases,
		MediaExperienceDatabases,
		MLCVDatabases,
		MLInfraDatabases,
		MobileDatabases,
		OemDatabases,
		OPXDatabases,
		PlatformAdminOpsDatabases,
		PlatformOperationsDatabases,
		PlatformReportsDatabases,
		ProductOperationsDatabases,
		ReleaseManagementDatabases,
		RoutingDatabases,
		SafetyADASDatabases,
		SafetyCameraPlatformDatabases,
		SafetyEventIngestionAndFilteringDatabases,
		SafetyEventReviewDevDatabases,
		SafetyPlatformDatabases,
		SafetyRiskDatabases,
		SafetyScoringInsightsDatabases,
		SalesDataAnalyticsDatabases,
		SolutionsIntegrationServicesDatabases,
		SREDatabases,
		SupplyChainDatabases,
		SupportDatabases,
		SupportOpsDatabases,
		SustainabilityDatabases,
		TelematicsDataDatabases,
		TestAutomationDatabases,
		TPMDatabases,
	}
}

func (db SamsaraDB) GetRegionsMap() map[string]bool {
	regions := make(map[string]bool)
	for _, awsAccountId := range db.AWSAccountIDs {
		region := infraconsts.AllSamsaraAWSAccountIDsDefaultRegionPrefix[awsAccountId].Region
		regions[region] = true
	}
	return regions
}

type DatabaseBucketType string

const (
	GlueBucket_LEGACY  DatabaseBucketType = "glue_legacy"
	UnityCatalogBucket DatabaseBucketType = "unity_catalog"
)

func (db SamsaraDB) GetS3Location(region string, bucketType DatabaseBucketType) (string, error) {
	dbBucket := db.Bucket
	if bucketType == GlueBucket_LEGACY {
		dbBucket = db.GetGlueBucket()
	}
	bucketPrefix := awsregionconsts.RegionPrefix[region]
	legacyDbNames := map[string]bool{
		"safety_map_data":  true,
		"dojo":             true,
		"labelbox":         true,
		"samsara_zendesk":  true,
		"netsuite_finance": true,
	}

	location := fmt.Sprintf(`s3://%s%s/%s.db`, bucketPrefix, dbBucket, db.Name)
	if _, ok := legacyDbNames[db.Name]; ok {
		location = fmt.Sprintf(`s3://%s%s/%s`, bucketPrefix, dbBucket, db.Name)
	}

	if db.DatabaseGroup == PlaygroundDatabaseGroup_LEGACY {
		location = fmt.Sprintf(`s3://%s%s/warehouse/%s.db`, bucketPrefix, dbBucket, db.Name)
	} else if db.DatabaseGroup == TeamDevDatabaseGroup {
		location = fmt.Sprintf(`s3://%s%s/team_dbs/%s.db`, bucketPrefix, dbBucket, db.Name)
	} else if db.DatabaseGroup == RdsDatabaseGroup {
		// Don't set a storage location for combined shards schemas. We don't expect any real tables here (only views).
		location = ""
	} else if db.DatabaseGroup == RdsShardsDatabaseGroup {
		// Ex: associations_shard_1db -> associations
		rdsRegistryName := rdsdeltalake.SparkDbNameToRdsDbRegistryName(db.Name)
		// Ex: associations_shard_1db -> prod-associations-shard-1db
		prodDbName, err := rdsdeltalake.ShardedSparkDbNameToProdDbName(db.Name)
		if err != nil {
			return "", oops.Wrapf(err, "Error converting spark db name %s to prod db name", db.Name)
		}
		// Set to the same location where the merge jobs output the replicated tables.
		location = fmt.Sprintf(`s3://%s%s/table-parquet/%s/%sdb`, bucketPrefix, dbBucket, prodDbName, rdsRegistryName)
	}
	return location, nil
}

func (db SamsaraDB) GetGlueBucket() string {
	if db.LegacyGlueS3Bucket != "" {
		return db.LegacyGlueS3Bucket
	}
	return db.Bucket
}

// TODO: Fix to only return the DBs that have tables in the given region
// Right now this returns all DBs in the registry, some of which may not
// have tables in a given region. This is fine for now but we should
// fix it by checking for `db.TablesInRegion(region) == nil`
func GetRdsDeltaLakeDbs() []SamsaraDB {
	var rdsDbs []SamsaraDB
	for _, db := range rdsdeltalake.AllDatabases() {
		dbName := db.Name
		if db.Sharded {
			dbName = fmt.Sprintf("%s_shards", db.Name)
		}
		canReadGroups := clusterhelpers.DatabricksClusterTeams()

		// If the database has a CanReadGroupsOverride, use that instead of the default.
		if len(db.CanReadGroups) > 0 {
			canReadGroups = db.CanReadGroups
		}

		canReadBiztechGroups := map[string][]string{}
		if len(db.CanReadBiztechGroups) > 0 {
			canReadBiztechGroups = db.CanReadBiztechGroups
		}

		rdsDbs = append(rdsDbs, SamsaraDB{
			Name:                 dbName,
			OwnerTeam:            team.DataPlatform,
			Bucket:               RdsDeltaLakeBucket,
			DatabaseGroup:        RdsDatabaseGroup,
			AWSAccountIDs:        RdsDatabaseAWSAccountRegions(db),
			CanReadGroups:        canReadGroups,
			CanReadBiztechGroups: canReadBiztechGroups,
		})
	}
	return rdsDbs
}

// Returns all Delta lake DBs for RDS in the specified region.
// Includes individual shard + combined shard DBs for RDS based on the region.
func GetRdsDeltaLakeDbsInRegion(region string) []SamsaraDB {
	var rdsDbs []SamsaraDB
	for _, db := range rdsdeltalake.AllDatabases() {
		if db.SkipSyncToHms {
			continue
		}
		if db.TablesInRegion(region) == nil {
			continue
		}
		canReadGroups := clusterhelpers.DatabricksClusterTeams()
		if len(db.CanReadGroups) > 0 {
			canReadGroups = db.CanReadGroups
		}
		canReadBiztechGroups := map[string][]string{}
		if len(db.CanReadBiztechGroups) > 0 {
			canReadBiztechGroups = db.CanReadBiztechGroups
		}

		for _, shard := range db.RegionToShards[region] {
			rdsDbs = append(rdsDbs, SamsaraDB{
				Name:                 rdsdeltalake.GetSparkFriendlyRdsDBName(shard, db.Sharded, true),
				OwnerTeam:            team.DataPlatform,
				Bucket:               RdsDeltaLakeBucket,
				DatabaseGroup:        RdsDatabaseGroup,
				AWSAccountIDs:        RdsDatabaseAWSAccountRegions(db),
				CanReadGroups:        canReadGroups,
				CanReadBiztechGroups: canReadBiztechGroups,
			})
		}
		if db.Sharded {
			dbName := fmt.Sprintf("%s_shards", db.Name)
			rdsDbs = append(rdsDbs, SamsaraDB{
				Name:                 dbName,
				OwnerTeam:            team.DataPlatform,
				Bucket:               RdsDeltaLakeBucket,
				DatabaseGroup:        RdsDatabaseGroup,
				AWSAccountIDs:        RdsDatabaseAWSAccountRegions(db),
				CanReadGroups:        canReadGroups,
				CanReadBiztechGroups: canReadBiztechGroups,
			})
		}
	}
	return rdsDbs
}

func RdsDatabaseAWSAccountRegions(rdsDb rdsdeltalake.RegistryDatabase) []string {
	var awsRegionAccounts []string
	if rdsDb.IsInRegion(infraconsts.SamsaraAWSDefaultRegion) {
		awsRegionAccounts = append(awsRegionAccounts, DatabricksAWSAccountUS)
	}
	if rdsDb.IsInRegion(infraconsts.SamsaraAWSEURegion) {
		awsRegionAccounts = append(awsRegionAccounts, DatabricksAWSAccountEU)
	}
	if rdsDb.IsInRegion(infraconsts.SamsaraAWSCARegion) {
		awsRegionAccounts = append(awsRegionAccounts, DatabricksAWSAccountCA)
	}
	return awsRegionAccounts
}

type GetDatabasesFilter string

const (
	// Specifies the group of DBs in the database registry prior to the UC migration.
	LegacyOnlyDatabases GetDatabasesFilter = "legacy_only"
	// Specifies *all* databases in the registry.
	AllUnityCatalogDatabases GetDatabasesFilter = "all_unity_catalog"
)

// GetAllDatabases returns all databases defined in the registry.
// Does not include individual shard DBs backing the RDS combined shards.
// Returns all DBs in the registry, some of which may not have tables in a given region.
// This should be fixed by modifying the GetRdsDeltaLakeDBs function to only return DBs with tables in a given region.
func GetAllDatabases(config dataplatformconfig.DatabricksConfig, databaseFilter GetDatabasesFilter) []SamsaraDB {
	var databases []SamsaraDB
	for _, teamDbs := range GetAllTeamDatabases(config) {
		databases = append(databases, teamDbs...)
	}
	// To get legacy only DBs, filter out DBs that were not in the legacy database registry because
	// they already exist in the Glue catalog. Otherwise, continue on to return all Unity Catalog DBs.
	if databaseFilter == LegacyOnlyDatabases {
		var legacyDatabases []SamsaraDB
		for _, db := range databases {
			if db.SkipGlueCatalog {
				continue
			}
			legacyDatabases = append(legacyDatabases, db)
		}
		return legacyDatabases
	}

	var ucDatabases []SamsaraDB
	for _, db := range databases {
		// Skip the legacy clouddb entry in the team registry. CloudDB is maintained in the rdsdeltalake registry
		// in the UC world. Once we move off of Glue, we can delete the legacy clouddb registry entry and clean-up this code.
		if db.Name == "clouddb" {
			continue
		}
		ucDatabases = append(ucDatabases, db)
	}
	return append(ucDatabases, GetRdsDeltaLakeDbs()...)
}

// GetAllBiztechGroupsInDefaultCatalog returns a slice of all the biztech groups in the default catalog.
// This is used to populate the use_catalog_group_names field in the default catalog.
func GetAllBiztechGroupsInDefaultCatalog(config dataplatformconfig.DatabricksConfig) []string {
	uniqueGroups := make(map[string]bool)
	for _, db := range GetAllDatabases(config, AllUnityCatalogDatabases) {
		for group, regions := range db.CanReadBiztechGroups {
			for _, region := range regions {
				if region == config.Region {
					uniqueGroups[group] = true
					break
				}
			}
		}
		for group, regions := range db.CanReadWriteBiztechGroups {
			for _, region := range regions {
				if region == config.Region {
					uniqueGroups[group] = true
					break
				}
			}
		}
		if db.OwnerTeamName != "" && IsValidBiztechGroupName(db.OwnerTeamName) {
			uniqueGroups[db.OwnerTeamName] = true
		}
	}
	result := make([]string, 0, len(uniqueGroups))
	for group := range uniqueGroups {
		result = append(result, group)
	}
	return result
}

// GetAllDBNamesSorted returns a slice of all the database names sorted.
// If excludeUnityDbs is true, only database names in the legacy
// database_registry (prior to the UC migration) are returned.
func GetAllDBNamesSorted(config dataplatformconfig.DatabricksConfig, databaseFilter GetDatabasesFilter) []string {
	var dbNames []string
	for _, db := range GetAllDatabases(config, databaseFilter) {
		dbNames = append(dbNames, db.Name)
	}
	sort.Strings(dbNames)
	return dbNames
}

func GetDatabaseByName(config dataplatformconfig.DatabricksConfig, dbName string, databaseFilter GetDatabasesFilter) (SamsaraDB, error) {
	for _, db := range GetAllDatabases(config, databaseFilter) {
		if db.Name == dbName {
			return db, nil
		}
	}
	return SamsaraDB{}, oops.Errorf("database %s not found in database registry with filter %s", dbName, databaseFilter)
}

func GetDatabasesByGroup(config dataplatformconfig.DatabricksConfig, group DatabaseGroup, databaseFilter GetDatabasesFilter) []SamsaraDB {
	var filteredDbs []SamsaraDB
	for _, db := range GetAllDatabases(config, databaseFilter) {
		if db.DatabaseGroup == group {
			filteredDbs = append(filteredDbs, db)
		}
	}
	return filteredDbs
}

func GetCustomBucketDatabases(config dataplatformconfig.DatabricksConfig, databaseFilter GetDatabasesFilter) []SamsaraDB {
	customDbs := append(GetDatabasesByGroup(config, CustomBucketGeneralDatabaseGroup, databaseFilter), GetDatabasesByGroup(config, CustomBucketDataModelDevelopmentNonGADatabaseGroup, databaseFilter)...)
	customDbs = append(customDbs, GetDatabasesByGroup(config, CustomBucketDataModelDevelopmentGADatabaseGroup, databaseFilter)...)
	customDbs = append(customDbs, GetDatabasesByGroup(config, CustomBucketDataModelProductionNonGADatabaseGroup, databaseFilter)...)
	customDbs = append(customDbs, GetDatabasesByGroup(config, CustomBucketDataModelProductionGADatabaseGroup, databaseFilter)...)
	return customDbs
}

type DataAccessLevel string

const (
	Read      DataAccessLevel = "read"
	ReadWrite DataAccessLevel = "readwrite"
)

// Returns a distinct set of buckets (with the region prefix) that exist in the specified region for the given databases.
func GetBucketsForDatabasesInRegion(config dataplatformconfig.DatabricksConfig, dbNames []string, accessLevel DataAccessLevel, databaseFilter GetDatabasesFilter) (buckets []string) {
	prefix := awsregionconsts.RegionPrefix[config.Region]
	uniqueBucketsMap := make(map[string]bool)
	desiredDbsMap := DBNamesToMap(dbNames)
	for _, db := range GetAllDatabases(config, databaseFilter) {
		// Match on database name, and check that the database exists in the specified region.
		if desiredDbsMap[db.Name] && db.GetRegionsMap()[config.Region] {
			// We don't grant blanket read-write access to databricks-workspace.
			// If team db read-write access is needed, only grant it for that specific db.
			teamDevDbNames := GetDBNames(GetDatabasesByGroup(config, TeamDevDatabaseGroup, databaseFilter))
			if _, ok := DBNamesToMap(teamDevDbNames)[db.Name]; ok && accessLevel == ReadWrite {
				teamDevBucket := fmt.Sprintf("%sdatabricks-workspace/team_dbs/%s.db", prefix, db.Name)
				uniqueBucketsMap[teamDevBucket] = true
			} else {
				bucket := db.Bucket
				if databaseFilter == LegacyOnlyDatabases {
					bucket = db.GetGlueBucket()
				}
				uniqueBucketsMap[prefix+bucket] = true
			}
		}
	}
	for bucket := range uniqueBucketsMap {
		buckets = append(buckets, bucket)
	}
	return buckets
}

// Function that takes a slice of DBs and returns a slice of the DB names
func GetDBNames(databases []SamsaraDB) []string {
	var dbNames []string
	for _, db := range databases {
		dbNames = append(dbNames, db.Name)
	}
	return dbNames
}

func DBNamesToMap(dbNames []string) map[string]bool {
	namesMap := make(map[string]bool)
	for _, name := range dbNames {
		namesMap[name] = true
	}
	return namesMap
}

func IsValidBiztechGroupName(groupName string) bool {
	// Biztech teams begin with "bt-" or "bted-" or "gtm-"
	return strings.HasPrefix(groupName, "bt-") || strings.HasPrefix(groupName, "bted-") || strings.HasPrefix(groupName, "gtm-")
}
