package dataplatformconsts

import (
	"fmt"

	"samsaradev.io/libs/ni/infraconsts"
)

const (
	// DatabricksDevUSShardName is the shard name for the dev-us-west-2 Databricks E2 workspace
	DatabricksDevUSShardName string = "oregon-prod"
	// DatabricksDevEUShardName is the shard name for the dev-eu-west-1 Databricks E2 workspace
	DatabricksDevEUShardName string = "ireland-prod"
	// DatabricksDevCAShardName is the shard name for the dev-ca-central-1 Databricks E2 workspace
	DatabricksDevCAShardName string = "canada-prod"
	// DatabricksDevUSWorkspaceID is the workspace id for the dev-us-west-2 Databricks E2 workspace
	DatabricksDevUSWorkspaceId int = 5924096274798303
	// DatabricksDevEUWorkspaceID is the workspace id for the dev-eu-west-2 Databricks E2 workspace
	DatabricksDevEUWorkspaceId int = 6992178240159315
	// DatabricksDevCAWorkspaceID is the workspace id for the dev-ca-central-1 Databricks E2 workspace
	DatabricksDevCAWorkspaceId int = 1976292512529253

	// Workspace ID for the Biztech POC workspace in the US region
	BiztechPocWorkspaceId int = 5058836858322111
)

type DataPlatformJobType string

const (
	KinesisStatsDeltaLakeIngestionMerge    DataPlatformJobType = "kinesisstats_deltalake_ingestion_merge"
	KinesisStatsDeltaLakeIngestionVacuum   DataPlatformJobType = "kinesisstats_deltalake_ingestion_vacuum"
	KinesisStatsDeltaLakeIngestionOptimize DataPlatformJobType = "kinesisstats_deltalake_ingestion_optimize"

	KinesisStatsBigStatsDeltaLakeIngestionMerge  DataPlatformJobType = "kinesisstats_bigstats_deltalake_ingestion_merge"
	KinesisStatsBigStatsDeltaLakeIngestionVacuum DataPlatformJobType = "kinesisstats_bigstats_deltalake_ingestion_vacuum"

	RdsDeltaLakeIngestionMerge         DataPlatformJobType = "rds_deltalake_ingestion_merge"
	RdsDeltaLakeIngestionVacuum        DataPlatformJobType = "rds_deltalake_ingestion_vacuum"
	RdsDeltaLakeIngestionCombineShards DataPlatformJobType = "rds_deltalake_ingestion_combine_shards"
	RdsDeltaLakeIngestionValidation    DataPlatformJobType = "rds_deltalake_ingestion_validation"

	DynamoDbDeltaLakeIngestionMerge  DataPlatformJobType = "dynamodb_deltalake_ingestion_merge"
	DynamoDbDeltaLakeIngestionVacuum DataPlatformJobType = "dynamodb_deltalake_ingestion_vacuum"

	EmrDeltaLakeIngestionMerge      DataPlatformJobType = "emr_deltalake_ingestion_merge"
	EmrDeltaLakeIngestionVacuum     DataPlatformJobType = "emr_deltalake_ingestion_vacuum"
	EmrDeltaLakeIngestionValidation DataPlatformJobType = "emr_deltalake_ingestion_validation"
	OrgShardsTableV2                DataPlatformJobType = "org_shards_table_v2"
	ReportAggregator                DataPlatformJobType = "report_aggregator"
	ReportSqliteAggregator          DataPlatformJobType = "report_sqlite_aggregator"

	// Note that the sql transformation job is currently created via a python lambda, hence it does
	// not have a const here
	DataPipelinesAudit      DataPlatformJobType = "data_pipelines_audit"
	DataPipelinesVacuum     DataPlatformJobType = "data_pipelines_vacuum"
	DataPipelinesSqliteNode DataPlatformJobType = "data_pipelines_sqlite_node"

	GeneralDbVacuum    DataPlatformJobType = "general_db_vacuum"
	GlueSnapshotBackup DataPlatformJobType = "glue_snapshot_backup"
	DeltaLakeDeletion  DataPlatformJobType = "delta_lake_deletion"
	UnityCatalogSync   DataPlatformJobType = "unity_catalog_sync"
	Mixpanel           DataPlatformJobType = "mixpanel"

	ScheduledNotebook  DataPlatformJobType = "scheduled_notebook"
	InteractiveCluster DataPlatformJobType = "interactive_cluster"
	SqlEndpoint        DataPlatformJobType = "sql_endpoint"
	Amundsen           DataPlatformJobType = "amundsen"
	Datahub            DataPlatformJobType = "datahub"
	Workflow           DataPlatformJobType = "workflow"
	Dagster            DataPlatformJobType = "dagster"
	SparkStreaming     DataPlatformJobType = "spark_streaming"

	SLO_TARGET_TAG               = "slo-target"
	LOW_URGENCY_THRESHOLD_TAG    = "low-urgency-threshold"
	HIGH_URGENCY_THRESHOLD_TAG   = "high-urgency-threshold"
	BUSINESS_HOURS_THRESHOLD_TAG = "business-hours-threshold"
	PRODUCTION_TAG               = "is-production-job"
	DATAPLAT_JOBTYPE_TAG         = "dataplatform-job-type"
	DATABASE_TAG                 = "database"
	TABLE_TAG                    = "table"
	SCHEDULE_TAG                 = "schedule"
	CELL_TAG                     = "cell"

	RndUserGroup    = "samsara-rnd"
	NonRndUserGroup = "samsara-nonrnd"
)

// These are the up-to-date R&D allocation values for the S3 buckets as of 2023-11-20
// The exact distribution of COGS vs OPEX data in the buckets is always fluctuating slightly
// but it will generally stay relatively constant so we run the following notebook:
// https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#notebook/957455983165964/command/957455983166215
// to calculate the values and update at a regular cadence to be determined
const (
	KinesisstatsDefaultPartitionChunkSize int = 20
	KinesisstatsDailyPartitionChunkSize   int = 50
	KinesisstatsDefaultS3FilesPerChunk    int = 25000

	KinesisstatsStorageRndAllocation float64 = 0.7122
	RdsStorageRndAllocation          float64 = 0.4675
	// most of Data Pipelines OPEX data corresponds to old data to be deleted
	// the value should be updated once those data are purged.
	DatapipelinesStorageRndAllocation    float64 = 0.1593
	S3bigstatsStorageRndAllocation       float64 = 1.0000
	DynamodbStorageRndAllocation         float64 = 1.0000
	DatastreamsRawStorageRndAllocation   float64 = 0.1858
	DatastreamsDeltaStorageRndAllocation float64 = 0.1866
)

var DatabricksNitroInstanceTypePrefixes = []string{
	"m-fleet", "md-fleet", "m5dn", "m5n", "m5zn", "m6i", "m7i", "m6id", "m6in", "m6idn", "m6a", "m7a",
	"c5a", "c5ad", "c5n", "c6i", "c6id", "c7i", "c6in", "c6a", "c7a",
	"r-fleet", "rd-fleet", "r6i", "r7i", "r7iz", "r6id", "r6in", "r6idn", "r6a", "r7a",
	"d3", "d3en", "p3dn", "r5dn", "r5n", "i4i", "i3en",
	"g4dn", "g5", "p4d", "p4de", "p5",
}

// Does not return BiztechPocWorkspaceId for now, as this is a POC workspace.
func GetDevDatabricksWorkspaceIds() []int {
	return []int{DatabricksDevUSWorkspaceId, DatabricksDevEUWorkspaceId, DatabricksDevCAWorkspaceId}
}

func GetDevDatabricksWorkspaceIdForRegion(region string) int {
	if region == infraconsts.SamsaraAWSDefaultRegion {
		return DatabricksDevUSWorkspaceId
	} else if region == infraconsts.SamsaraAWSEURegion {
		return DatabricksDevEUWorkspaceId
	} else if region == infraconsts.SamsaraAWSCARegion {
		return DatabricksDevCAWorkspaceId
	}
	return 0
}

type KsJobType string

const (
	KsJobTypeAll      = KsJobType("all")
	KsJobTypeEvery1Hr = KsJobType("every-1hr")
	KsJobTypeEvery3Hr = KsJobType("every-3hr")
	KsJobTypeDaily    = KsJobType("daily")
)

type JobSlo struct {
	// This is the target in hours in our SLO for the job.
	SloTargetHours int64

	// The urgency with which we act on this SLO.
	Urgency JobSloUrgency

	// The thresholds we page on don't usually match exactly
	// the SLO target. Monitoring jobs based only on SLO
	// is likely not going to work well for our use cases.
	// We allow configuring a threshold for:
	// - low urgency
	// - high urgency business hours
	// - high urgency (all times)
	LowUrgencyThresholdHours    int64
	BusinessHoursThresholdHours int64
	HighUrgencyThresholdHours   int64
}

func (j JobSlo) SloDescription() string {
	description := ""
	if j.SloTargetHours > 0 {
		responseString := "and no guarantee on response"
		if j.Urgency == JobSloBusinessHoursHigh {
			responseString = "and responded to from 8am-4pm PT on weekdays"
		} else if j.Urgency == JobSloHigh {
			responseString = "and responded to 24/7"
		}

		description += fmt.Sprintf("Data Freshness: This table will be at least %d hours fresh, with breaches posted to #data-platform-status %s.", j.SloTargetHours, responseString)
	}

	return description
}

type JobSloUrgency int64

const (
	JobSloLow JobSloUrgency = iota
	JobSloBusinessHoursHigh
	JobSloHigh
)

const (
	LowUrgencyThreshold    = "low_urgency_threshold"
	BusinessHoursThreshold = "business_hours_threshold"
	HighUrgencyThreshold   = "high_urgency_threshold"
	SloTarget              = "slo_target"
)

// https://docs.databricks.com/en/data-governance/unity-catalog/create-metastore.html
const DatabricksUCAwsRoleReference = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"

// Databricks has created an incredibly confusing authentication scheme for storage credentials.
// https://docs.databricks.com/en/connect/unity-catalog/storage-credentials.html
// When a storage credential is created, there is an external id that is associated with it, and is
// necessary to be included in the trust policy of the IAM role.
// Normally, this would require a multi-step process that is basically impossible to manage via
// terraform without manual intervention, but according to databricks support, as long as an
// admin creates the storage credential, this external id will be the same.
// I confirmed this value by creating a storage credential in the UI and then checking the value.
// We'll have to repeat this for every region.
const (
	USStorageCredentialExternalId = "f8e9c6b3-6083-4e24-bf92-19bbd19235e3"

	// It's not a typo that this is the same as the US, this is truly what databricks gave us.
	EUStorageCredentialExternalId = "f8e9c6b3-6083-4e24-bf92-19bbd19235e3"

	CAStorageCredentialExternalId = "f8e9c6b3-6083-4e24-bf92-19bbd19235e3"
)

func StorageCredentialIamRoleName(bucketName string) string {
	return fmt.Sprintf("uc-%s", bucketName)
}
func CloudCredentialIamRoleName(name string) string {
	return fmt.Sprintf("cloud-cred-%s", name)
}
func GetCredentialExternalId(region string) (string, error) {
	switch region {
	case infraconsts.SamsaraAWSDefaultRegion:
		return USStorageCredentialExternalId, nil
	case infraconsts.SamsaraAWSEURegion:
		return EUStorageCredentialExternalId, nil
	case infraconsts.SamsaraAWSCARegion:
		return CAStorageCredentialExternalId, nil
	}
	return "", fmt.Errorf("no external id for region %s", region)
}

type Parameter string

const (
	ParameterSlackAppToken               = Parameter("SLACK_APP_TOKEN")
	ParameterDagsterSlackBotToken        = Parameter("DAGSTER_SLACK_BOT_TOKEN")
	ParameterDatabricksEuToken           = Parameter("DAGSTER_DATABRICKS_EU_TOKEN")
	ParameterDatadogAppToken             = Parameter("DATADOG_APP_KEY")
	ParameterDatadogApiToken             = Parameter("DATADOG_API_KEY")
	ParameterDatahubToken                = Parameter("DATAHUB_GMS_TOKEN")
	ParameterDatahubServer               = Parameter("DATAHUB_GMS_SERVER")
	ParameterGoogleMapsApiToken          = Parameter("DISPATCH_ROUTES_GOOGLE_MAPS_KEY")
	ParameterDMV2RolloutLDKey            = Parameter("DM_V2_ROLLOUT_LD_KEY")
	ParameterGreenhouseApiToken          = Parameter("GREENHOUSE_HARVEST_TOKEN")
	ParameterTomTomApiToken              = Parameter("TOMTOM_API_KEY")
	ParameterJdmFtpSshPrivateKey         = Parameter("SAMSARA_JDM_FTP_SSH_PRIVATE_KEY")
	ParameterHereMapsApiToken            = Parameter("HERE_MAPS_API_TOKEN")
	ParameterPagerDutyDELowUrgencyKey    = Parameter("PAGERDUTY_DE_LOW_URGENCY_KEY")
	ParameterDatahubDbtApiToken          = Parameter("DATAHUB_DBT_API_TOKEN")
	ParameterDatahubTableauToken         = Parameter("DATAHUB_TABLEAU_TOKEN")
	ParameterDagsterDatabricksClientId   = Parameter("DAGSTER_DATABRICKS_CLIENTID")
	ParameterDagsterDatabricksSecret     = Parameter("DAGSTER_DATABRICKS_SECRET")
	ParameterDagsterDatabricksEuClientId = Parameter("DAGSTER_DATABRICKS_EU_CLIENTID")
	ParameterDagsterDatabricksEuSecret   = Parameter("DAGSTER_DATABRICKS_EU_SECRET")
	ParameterDagsterDatabricksCaClientId = Parameter("DAGSTER_DATABRICKS_CA_CLIENTID")
	ParameterDagsterDatabricksCaSecret   = Parameter("DAGSTER_DATABRICKS_CA_SECRET")

	// Marketing Data Analytics keys
	ParameterMarketingDataAnalyticsSegmentDevWriteKey         = Parameter("SEGMENT_DEV_WRITE_KEY")
	ParameterMarketingDataAnalyticsSegmentProdWriteKey        = Parameter("SEGMENT_PROD_WRITE_KEY")
	ParameterMarketingDataAnalyticsBigQueryDevPrivateId       = Parameter("BIGQUERY_DEV_PRIVATE_ID")
	ParameterMarketingDataAnalyticsBigQueryProdPrivateId      = Parameter("BIGQUERY_PROD_PRIVATE_ID")
	ParameterMarketingDataAnalyticsBigQueryDevPrivateKey      = Parameter("BIGQUERY_DEV_PRIVATE_KEY")
	ParameterMarketingDataAnalyticsBigQueryProdPrivateKey     = Parameter("BIGQUERY_PROD_PRIVATE_KEY")
	ParameterMarketingDataAnalyticsMixpanelDevProjectToken    = Parameter("MIXPANEL_DEV_PROJECT_TOKEN")
	ParameterMarketingDataAnalyticsMixpanelProdProjectToken   = Parameter("MIXPANEL_PROD_PROJECT_TOKEN")
	ParameterMarketingDataAnalyticsMixpanelDevAPISecret       = Parameter("MIXPANEL_DEV_API_SECRET")
	ParameterMarketingDataAnalyticsMixpanelProdAPISecret      = Parameter("MIXPANEL_PROD_API_SECRET")
	ParameterMarketingDataAnalyticsOn24DevAccessTokenKey      = Parameter("ON24_DEV_ACCESS_TOKEN_KEY")
	ParameterMarketingDataAnalyticsOn24ProdAccessTokenKey     = Parameter("ON24_PROD_ACCESS_TOKEN_KEY")
	ParameterMarketingDataAnalyticsOn24DevAccessTokenSecret   = Parameter("ON24_DEV_ACCESS_TOKEN_SECRET")
	ParameterMarketingDataAnalyticsOn24ProdAccessTokenSecret  = Parameter("ON24_PROD_ACCESS_TOKEN_SECRET")
	ParameterMarketingDataAnalyticsSalesforceDevUsername      = Parameter("SALESFORCE_DEV_USERNAME")
	ParameterMarketingDataAnalyticsSalesforceDevPassword      = Parameter("SALESFORCE_DEV_PASSWORD")
	ParameterMarketingDataAnalyticsSalesforceDevSecurityToken = Parameter("SALESFORCE_DEV_SECURITY_TOKEN")
	ParameterMarketingDataAnalyticsIterableDevAPIToken        = Parameter("ITERABLE_DEV_API_TOKEN")
	ParameterMarketingDataAnalyticsIterableProdAPIToken       = Parameter("ITERABLE_PROD_API_TOKEN")
)
