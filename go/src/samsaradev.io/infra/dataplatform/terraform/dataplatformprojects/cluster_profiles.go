package dataplatformprojects

import (
	"fmt"
	"sort"
	"strings"

	"github.com/samsarahq/go/oops"

	"samsaradev.io/infra/dataplatform/dataplatformhelpers/clusterhelpers"
	"samsaradev.io/infra/dataplatform/ni/dataplatformconsts"
	"samsaradev.io/infra/dataplatform/terraform/dataplatformconfig"
	"samsaradev.io/infra/dataplatform/unitycatalog/databaseregistries"
	"samsaradev.io/infra/samsaraaws/awsregionconsts"
	"samsaradev.io/libs/ni/infraconsts"
	"samsaradev.io/team"
	"samsaradev.io/team/components"
)

type ClusterProfile struct {
	clusterName         string
	instanceProfileName string
	canCreateDatabases  bool
	policyAttachments   []string

	// Grants Glue and S3 bucket access to databases.
	readDatabases      []string
	readwriteDatabases []string

	// Grants specialized S3 access to buckets outside the readDatabases and readwriteDatabases.
	// You do not need to specify read/readwrite bucket access in these fields if there is a database
	// listed in readDatabases/readwriteDatabases, respectively, that lives in that bucket. S3 bucket
	// access for databases is handled automatically.
	readBuckets      []string
	readwriteBuckets []string
	// Note: Region specific buckets should include the bucket prefix on the bucket string.
	regionSpecificReadBuckets      map[string][]string
	regionSpecificReadWriteBuckets map[string][]string

	sqsConsumerQueues []string
	sqsProducerQueues []string
	readParameters    []dataplatformconsts.Parameter

	// TODO: Deprecate the teamReadWriteDatabases field in a followup PR. Merge with the s3_database_write policy.
	teamReadWriteDatabases []string // DO NOT SET. This is for terraform generation only.

	// A flag to indicate whether or not this is for unity catalog.
	// For unity catalog instance profiles, we want to reduce a lot of the
	// default permission sets
	isForUnityCatalog bool
}

var standardDevReadBuckets = []string{ // lint: +sorted
	"att-sftp",
	"benchmarking-metrics",
	"data-eng-mapping-tables",
	"data-insights",
	"data-pipelines-delta-lake",
	"data-science-exploratory",
	"data-stream-lake",
	"data-streams-delta-lake",
	"databricks-billing",
	"databricks-s3-inventory",
	"databricks-sql-query-history",
	"databricks-warehouse",
	"datahub-metadata",
	"devmetrics",
	"dss-ml-models",
	"dynamodb-delta-lake",
	"dynamodb-export",
	"fivetran-incidentio-delta-tables",
	"fivetran-pagerduty-delta-tables",
	"kinesisstats-delta-lake",
	"kinesisstats-json-export",
	"mixpanel",
	"partial-report-aggregation",
	"prod-app-configs",
	"rds-delta-lake",
	"rds-export",
	"report-staging-tables",
	"s3-inventory",
	"s3bigstats-delta-lake",
	"s3bigstats-json-export",
	"year-review-metrics",
	"zendesk",
}

var standardDevReadWriteBuckets = []string{ // lint: +sorted
	"databricks-playground",
}

// The config below for fivetran is used for clusters that fivetran
// connects to to sync data into databricks.
var fivetranReadWriteDatabases = []string{
	"_fivetran_setup_test",
	"_fivetran_staging",
	"fivetran_log", // fivetran_log is a free and optional connector you can set up that sends logs about each other connection
	"fivetranconnectortest",
	"fivetranconnectordemo",
}

func getPlaygroundBucketDbNames(config dataplatformconfig.DatabricksConfig) []string {
	return databaseregistries.GetDBNames(databaseregistries.GetDatabasesByGroup(
		config,
		databaseregistries.PlaygroundDatabaseGroup_LEGACY,
		databaseregistries.LegacyOnlyDatabases,
	))
}

func getDataPipelineEUReplicatedBucketsForUSRegion() []string {
	var buckets []string
	for _, region := range infraconsts.DONOTUSE_AllRegions {
		if region == infraconsts.SamsaraAWSDefaultRegion {
			continue
		}
		buckets = append(buckets, fmt.Sprintf("samsara-%s", GetDataPipelineReplicatedBucketNameForRegion(region)))
	}
	return buckets
}

// The list of buckets that are partial replicas of the data pipelines buckets from non US regions.
// These buckets are buckets in the US where if `replicated_to_us_from_regions` is set on a node, then its
// data is replicated to the US from the region specified.
// We want these buckets to be accessible to the US workspace.
var dataPipelineEUReplicatedBucketsForUSRegion = getDataPipelineEUReplicatedBucketsForUSRegion()

var standardReadParameters = []dataplatformconsts.Parameter{
	dataplatformconsts.ParameterSlackAppToken,
	dataplatformconsts.ParameterDatadogAppToken,
	dataplatformconsts.ParameterDatadogApiToken,
	dataplatformconsts.ParameterGoogleMapsApiToken,
}

func getCustomClusterProfiles(config dataplatformconfig.DatabricksConfig) []ClusterProfile {
	return []ClusterProfile{
		{
			clusterName:         "dataprep",
			instanceProfileName: "dataprep-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterSlackAppToken,
				dataplatformconsts.ParameterGoogleMapsApiToken,
				dataplatformconsts.ParameterGreenhouseApiToken,
			},
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readBuckets: append([]string{
				"bigdata-datashare",
				"datasets",
				"workforce-data",
				"netsuite-delta-tables",
				"databricks-workspace",
			},
				standardDevAndSensitiveReadBuckets...,
			),
			regionSpecificReadBuckets: map[string][]string{
				infraconsts.SamsaraAWSDefaultRegion: dataPipelineEUReplicatedBucketsForUSRegion,
			},
			readwriteBuckets: []string{ // lint: +sorted
				"awsbilluploader",
				"benchmarking-metrics",
				"data-science-exploratory",
				"detailed-ifta-reports",
			},
			readDatabases: []string{"netsuite_suiteanalytics"},
			readwriteDatabases: []string{
				"api_logs",
				"customer360",
				"canonical_distance_updates",
				"compliance",
				"data_analytics",
				"databricks_alerts",
				"dataengineering",
				"dataprep",
				"dataprep_cellular",
				"dataprep_safety",
				"dataprep_assets",
				"dataprep_firmware",
				"dataprep_ml",
				"dataprep_routing",
				"dataprep_telematics",
				"dataproducts",
				"datascience",
				"devexp",
				"firmware",
				"fueldb_shards",
				"fuel_maintenance",
				"ifta_report",
				"mixpanel",
				"mobile_logs",
				"routeload_logs",
				"zendesk",
			},
		},
		{
			clusterName:         "dev",
			instanceProfileName: "dev-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterSlackAppToken,
				dataplatformconsts.ParameterDatadogAppToken,
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterGoogleMapsApiToken,
			},
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
			},
			readBuckets: []string{
				"spark-playground",
			},
			regionSpecificReadBuckets: map[string][]string{
				infraconsts.SamsaraAWSDefaultRegion: dataPipelineEUReplicatedBucketsForUSRegion,
			},
			readwriteDatabases: getPlaygroundBucketDbNames(config),
		},
		{
			clusterName:         "kinesisstats-import",
			instanceProfileName: "kinesisstats-import-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			sqsConsumerQueues: []string{
				"samsara_delta_lake_merge_ks_*",
			},
			readBuckets: []string{
				"kinesisstats-json-export",
			},
			readwriteBuckets: []string{
				"kinesisstats-delta-lake",
				"databricks-job-resources",
				"databricks-workspace",
			},
			readwriteDatabases: []string{
				"auditlog",
			},
		},
		{
			clusterName:         "s3bigstats-import",
			instanceProfileName: "s3bigstats-import-cluster",
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			sqsConsumerQueues: []string{
				"samsara_delta_lake_merge_s3_big_stat_*",
			},
			readBuckets: []string{
				"s3bigstats-json-export",
				"databricks-job-resources",
			},
			readwriteBuckets: []string{
				"kinesisstats-delta-lake",
				"s3bigstats-delta-lake",
				"databricks-warehouse",
			},
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
		},
		{
			clusterName:         "readonly",
			instanceProfileName: "readonly-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readBuckets: []string{
				"kinesisstats-delta-lake",
				"rds-export",
			},
		},
		{
			clusterName:         "report-aggregation",
			instanceProfileName: "report-aggregation-cluster",
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			readBuckets: []string{
				"kinesisstats-delta-lake",
				"rds-delta-lake",
				"data-pipelines-delta-lake",
				"rds-export",
				"prod-app-configs",
			},
			readwriteBuckets: []string{
				"report-staging-tables",
				"partial-report-aggregation",
				"databricks-warehouse",
			},
		},
		{
			clusterName:         "rate-plan-optimization",
			instanceProfileName: "rate-plan-optimization-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
			},
			readwriteBuckets: []string{
				"jasper-rate-plan-optimization",
				"databricks-warehouse",
				"databricks-playground",
			},
		},
		{
			clusterName:         "thor-variant-ingest",
			instanceProfileName: "thor-variant-ingest-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterJdmFtpSshPrivateKey,
			},
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
			},
			readwriteBuckets: []string{
				"databricks-playground",
				"thor-variants",
			},
			readwriteDatabases: []string{
				"dataprep_firmware",
			},
		},
		{
			clusterName:         "vodafone-reports",
			instanceProfileName: "vodafone-reports-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
			},
			readwriteBuckets: []string{
				"vodafone-reports",
				"databricks-playground",
			},
			readwriteDatabases: []string{
				"dataprep_cellular",
			},
		},
		{
			clusterName:         "rds-import",
			instanceProfileName: "rds-import-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			sqsConsumerQueues: []string{
				"samsara_delta_lake_rds_*",
			},
			readBuckets: []string{
				"rds-export",
				"prod-app-configs",
			},
			readwriteBuckets: []string{
				"rds-delta-lake",
				"databricks-job-resources",
				"databricks-workspace",
			},
			readwriteDatabases: []string{
				"auditlog",
			},
		},
		{
			clusterName:         "dynamodb-import",
			instanceProfileName: "dynamodb-import-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			readBuckets: []string{
				"dynamodb-export",
			},
			readwriteBuckets: []string{
				"dynamodb-delta-lake",
				"databricks-warehouse",
				"databricks-job-resources",
				"databricks-workspace",
			},
		},
		{
			clusterName:         "admin",
			instanceProfileName: "admin-cluster",
			policyAttachments: []string{
				"biztech_s3_bucket_read_write",
				"glue_catalog_admin",
				"s3_standard_dev_buckets_read",
				"datamodel_dev_write",
				"datamodel_prod_write",
			},
			readBuckets: []string{ // lint: +sorted
				"biztech-edw-bronze",
				"biztech-edw-ci",
				"biztech-edw-dev",
				"biztech-edw-finops-sensitive",
				"biztech-edw-gold",
				"biztech-edw-peopleops-sensitive",
				"biztech-edw-sensitive",
				"biztech-edw-sensitive-bronze",
				"biztech-edw-sensitive-dev",
				"biztech-edw-silver",
				"biztech-edw-sterling",
				"biztech-edw-uat",
				"biztech-sda-production",
				"databricks",
				"databricks-audit-log",
				"databricks-s3-inventory",
				"databricks-s3-logging",
				"databricks-sql-query-history",
				"dataplatform-fivetran-bronze",
				"fivetran-greenhouse-recruiting-delta-tables",
				"fivetran-netsuite-finance-delta-tables",
				"fivetran-samsara-source-qa",
				"marketing-data-analytics",
				"ml-feature-store",
				"netsuite-delta-tables",
				"s3-inventory",
				"spark-playground",
				"staging-prediction-store",
			},
			regionSpecificReadBuckets: map[string][]string{
				infraconsts.SamsaraAWSDefaultRegion: dataPipelineEUReplicatedBucketsForUSRegion,
			},
			readwriteBuckets: []string{
				"databricks-playground",
				"kinesisstats-delta-lake",
				"partial-report-aggregation",
				"rds-delta-lake",
				"report-staging-tables",
				"databricks-warehouse",
				"databricks-workspace",
				"data-pipelines-delta-lake",
				"s3bigstats-delta-lake",
				"amundsen-metadata",
				"databricks-kinesisstats-diffs",
				"databricks-rds-diffs",
				"data-streams-delta-lake",
			},
			readDatabases: []string{
				"labelbox",
				"netsuite_finance",
				"safety_map_data",
				"supplychain",
			},
			readwriteDatabases: []string{
				"datastreams",
				"dojo",
				"labelbox",
				"dojo_tmp",
				"samsara_zendesk",
				"safety_map_data",
			},
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
		},
		{
			clusterName:         "data-pipelines",
			instanceProfileName: "data-pipelines-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
			policyAttachments: []string{
				"glue_catalog_admin",
				"s3_standard_dev_buckets_read",
			},
			readBuckets: []string{"databricks-playground"},
			regionSpecificReadBuckets: map[string][]string{
				infraconsts.SamsaraAWSDefaultRegion: dataPipelineEUReplicatedBucketsForUSRegion,
			},
			readwriteBuckets: []string{
				"data-pipelines-delta-lake",
				"databricks-playground",
				"databricks-warehouse",
				"zendesk",
				"databricks-job-resources",
			},
		},
		{
			clusterName:         "netsuiteprocessor",
			instanceProfileName: "netsuiteprocessor-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
			},
			readBuckets: []string{
				"netsuite-invoice-delta-tables", // the old bucket where netsuite data (invoices only) was sent to.
				"netsuite-delta-tables",
				"netsuite-sam-reports",
			},
			readwriteBuckets: []string{
				"databricks-netsuite-invoice-output",
				"netsuite-invoice-exports",
				"netsuite-licenses",
				"platops-databricks-metadata",
			},
			readwriteDatabases: []string{
				"netsuite_data",
				"platops",
			},
			sqsProducerQueues: []string{
				"samsara_netsuiteinvoiceworker_*",
				"samsara_eu_netsuiteinvoiceworker_*",
				"samsara_licenseingestionworker_*",
				"samsara_eu_licenseingestionworker_*",
				"samsara_exchangeingestionworker_*",
				"samsara_eu_exchangeingestionworker_*",
				"samsara_netsuite_record_delete_*",
			},
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogAppToken,
				dataplatformconsts.ParameterDatadogApiToken,
			},
		},
		{
			clusterName:         "netsuitefivetran",
			instanceProfileName: "netsuitefivetran-cluster",
			canCreateDatabases:  true,
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				// This DB is created by the netsuite connector in fivetran to sync the tables
				"netsuite_suiteanalytics",
				"netsuite_suiteanalytics_for_connection_all_data",
				"fivetran_phoenix_logarithmic_staging",
				"fivetran_spearman_beside_staging",
				"fivetran_silvery_irresponsible_staging",
			),
			readwriteBuckets: []string{
				"netsuite-delta-tables",
				"databricks-playground",
				"databricks-warehouse",
			},
		},
		{
			clusterName:         "fivetran-netsuite-finance",
			instanceProfileName: "fivetran-netsuite-finance-cluster",
			canCreateDatabases:  true,
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				// This DB is created by the netsuite connector in fivetran to sync the tables
				"fivetran_netsuite_finance",
				"fivetran_phoenix_logarithmic_staging",
				"fivetran_spearman_beside_staging",
				"fivetran_silvery_irresponsible_staging",
			),
			readwriteBuckets: []string{
				"fivetran-netsuite-finance-delta-tables", // where Fivetran syncs data to
				"databricks-warehouse",
				"databricks-playground", // _fivetran_setup_test is set up under the playground bucket for some reason
			},
		},
		{
			clusterName:         "fivetran-rds-replication",
			instanceProfileName: "fivetran-rds-replication-cluster",
			canCreateDatabases:  true,
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readwriteDatabases: fivetranReadWriteDatabases,
			readwriteBuckets: []string{
				"databricks-workspace", // where Fivetran syncs data to
				"databricks-warehouse", // _fivetran_setup_test is set up under the playground bucket for some reason
				"databricks-playground",
			},
		},
		{
			clusterName:         "netsuite-finance",
			instanceProfileName: "netsuite-finance-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readDatabases: []string{
				"fivetran_netsuite_finance",
				"netsuite_suiteanalytics",
			},
			readBuckets: []string{
				"fivetran-netsuite-finance-delta-tables",
				"netsuite-delta-tables",
			},
			readwriteDatabases: []string{
				"netsuite_finance",
				"fivetran_silvery_irresponsible_staging",
				"fivetran_spearman_beside_staging",
			},
			readwriteBuckets: []string{
				"netsuite-finance",
				"databricks-warehouse",
				"databricks-workspace/businesssystems",
			},
		},
		{
			clusterName:         "biztech-edw-uat",
			instanceProfileName: "biztech-edw-uat-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readwriteDatabases: []string{
				"biztech_edw_uat",
			},
			readBuckets: []string{
				"biztech-edw-dev",
				"netsuite-finance",
				"biztech-edw-bronze",
				"netsuite-delta-tables",
				"databricks-playground",
				"databricks-warehouse",
				"fivetran-netsuite-finance-delta-tables",
			},
		},
		{
			clusterName:         "biztech-edw-dev",
			instanceProfileName: "biztech-edw-dev-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
			},

			readwriteDatabases: []string{
				"biztech_edw_dev",
				"kpatro_poc_biztech_edw_bronze",
				"kpatro_poc_biztech_edw_silver",
				"kpatro_poc_biztech_edw_gold",
			},
			readBuckets: []string{
				"biztech-edw-bronze",
				"netsuite-finance",
				"netsuite-delta-tables",
				"databricks-playground",
				"databricks-warehouse",
				"fivetran-netsuite-finance-delta-tables",
			},

			regionSpecificReadWriteBuckets: map[string][]string{
				infraconsts.SamsaraAWSDefaultRegion: {
					"samsara-databricks-workspace/businesssystems",
				},
			},
		},
		{
			clusterName:         "biztech-sensitive-dbt-dev",
			instanceProfileName: "biztech-sensitive-dbt-dev-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readwriteDatabases: []string{
				"biztech_edws_galtobelli",
				"biztech_edws_uat",
			},
			readBuckets: []string{
				"biztech-edw-bronze",
				"biztech-edw-silver",
				"biztech-edw-gold",
				"biztech-edw-sensitive-bronze",
				"biztech-edw-peopleops-sensitive",
				"samsara-biztech-edw-sensitive",
			},
		},
		{
			clusterName:         "biztech-sensitive-dbt-prod",
			instanceProfileName: "biztech-sensitive-dbt-prod-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"glue_playground_bucket_databases_write",
				"s3_standard_dev_buckets_read",
				"s3_standard_dev_buckets_write",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				"biztech_edws_ci",
				"biztech_edws_greenhouse_bronze",
				"fivetran_backspace_compatible_staging",
			),
			readwriteBuckets: []string{
				"databricks-workspace/team_dbs",
			},
			readBuckets: []string{
				"biztech-edw-silver",
				"biztech-edw-bronze",
				"fivetran-greenhouse-recruiting-delta-tables",
			},
		},
		{
			clusterName:         "biztech-dbt-prod",
			instanceProfileName: "biztech-dbt-prod-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"glue_playground_bucket_databases_write",
				"s3_standard_dev_buckets_read",
				"s3_standard_dev_buckets_write",
			},
			readDatabases: []string{
				"biztech_edw_dev",
				"biztech_edw_uat",
			},
			readBuckets: []string{
				"biztech-edw-uat",
				"databricks-playground",
			},
			readwriteBuckets: []string{
				"netsuite-finance",
				"netsuite-invoice-delta-tables",
				"netsuite-delta-tables",
				"fivetran-netsuite-finance-delta-tables",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				"biztech_edw_bronze",
				"biztech_edw_silver",
				"biztech_edw_salesforce_silver",
				"biztech_edw_netsuite_silver",
				"biztech_edw_sterling",
				"biztech_edw_customer_success_gold",
				"biztech_edw_dataquality_gold",
				"netsuite_suiteanalytics",
				"biztech_edw_extracts_gold",
				"biztech_edw_bqmig_uat",
			),
			regionSpecificReadBuckets: map[string][]string{
				infraconsts.SamsaraAWSDefaultRegion: {
					"samsara-eu-biztech-edw-gold", // Grant the US region access to the EU bucket.
				},
				infraconsts.SamsaraAWSEURegion: {
					"samsara-biztech-edw-silver", // Grant the EU region access to the US bucket.
				},
			},
		},
		{
			clusterName:         "biztech-dbt-ci",
			instanceProfileName: "biztech-dbt-ci-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"glue_playground_bucket_databases_write",
				"s3_standard_dev_buckets_read",
				"s3_standard_dev_buckets_write",
			},
			readBuckets: []string{
				"biztech-edw-bronze",
				"biztech-edw-silver",
				"biztech-edw-sterling",
				"biztech-edw-gold",
				"netsuite-finance",
				"netsuite-invoice-delta-tables",
				"netsuite-delta-tables",
				"databricks-playground",
				"fivetran-netsuite-finance-delta-tables",
				"biztech-edw-dev",
				"biztech-edw-uat",
			},
			readwriteDatabases: []string{
				"biztech_edw_ci",
			},
			regionSpecificReadBuckets: map[string][]string{
				infraconsts.SamsaraAWSEURegion: {
					"samsara-biztech-edw-silver", // Grant the EU region access to the US bucket.
				},
			},
		},
		{
			clusterName:         "fivetran-biztech-edw",
			instanceProfileName: "fivetran-biztech-edw-cluster",
			canCreateDatabases:  true,
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				"netsuite_suiteanalytics",
				"fivetran_bacterium_rugged_staging",
				"fivetran_bonelike_encourage_staging",
				"fivetran_consoling_inhibition_staging",
				"fivetran_excel_inguinal_staging",
				"fivetran_longitudinal_aflame_staging",
				"fivetran_monorail_nineteen_staging",
				"fivetran_numbered_undermost_staging",
				"fivetran_petunia_background_staging",
				"fivetran_phoenix_logarithmic_staging",
				"fivetran_rural_prepaid_staging",
				"fivetran_shelf_cholera_staging",
				"fivetran_silvery_irresponsible_staging",
				"fivetran_spearman_beside_staging",
				"fivetran_viewable_torn_staging",
			),
			regionSpecificReadWriteBuckets: map[string][]string{
				infraconsts.SamsaraAWSDefaultRegion: {
					"samsara-databricks-playground", // _fivetran_setup_test is set up under the playground bucket for some reason
				},
			},
		},
		{
			clusterName:         "biztech-edw-silver",
			instanceProfileName: "biztech-edw-silver-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
			},
			readBuckets: []string{
				"biztech-edw-bronze",
				"databricks-playground",
			},
			readwriteDatabases: []string{
				"biztech_edw_silver",
				"dbt_poc_biztech_edw_silver",
			},
			regionSpecificReadWriteBuckets: map[string][]string{
				infraconsts.SamsaraAWSDefaultRegion: {
					"samsara-databricks-warehouse",
					"samsara-databricks-workspace/businesssystems",
				},
			},
		},
		{
			clusterName:         "zendeskfivetran",
			instanceProfileName: "zendeskfivetran-cluster",
			canCreateDatabases:  true,
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readwriteBuckets: []string{ // lint: +sorted
				"databricks-playground",
				"databricks-warehouse",
				"zendesk",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				"samsara_zendesk",
				"fivetran_petunia_background_staging",
			),
		},
		{
			clusterName:         "fivetran-greenhouse-recruiting",
			instanceProfileName: "fivetran-greenhouse-recruiting-cluster",
			canCreateDatabases:  true,
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readDatabases: []string{
				"biztech_edw_silver",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				// This DB is created by the greenhouse connector in fivetran to sync the tables
				"fivetran_greenhouse_recruiting",
				"people_analytics",
				"shared_people_ops_data",
				"supportops",
			),
			readwriteBuckets: []string{
				"fivetran-greenhouse-recruiting-delta-tables", // where Fivetran syncs data to
				"databricks-playground",                       // _fivetran_setup_test is set up under the playground bucket for some reason
			},
		},
		{
			clusterName:         "fivetran-samsara-source-qa",
			instanceProfileName: "fivetran-samsara-source-qa-cluster",
			canCreateDatabases:  true,
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				// This DB is created by the netsuite connector in fivetran to sync the tables
				"fivetran-samsara-source-qa",
			),
			readwriteBuckets: []string{
				"fivetran-samsara-source-qa", // where Fivetran syncs data to
				"databricks-warehouse",
				"databricks-playground", // _fivetran_setup_test is set up under the playground bucket for some reason
			},
		},
		{
			clusterName:         "fivetran-samsara-connector-demo",
			instanceProfileName: "fivetran-samsara-connector-demo-cluster",
			canCreateDatabases:  true,
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				// This DB is created by the netsuite connector in fivetran to sync the tables
				"fivetran-samsara-connector-demo",
			),
			readwriteBuckets: []string{
				"fivetran-samsara-connector-demo", // where Fivetran syncs data to
				"databricks-warehouse",
				"databricks-playground", // _fivetran_setup_test is set up under the playground bucket for some reason
			},
		},
		{
			clusterName:         "datahub-metadata-extraction",
			instanceProfileName: "datahub-metadata-extraction-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
				"datamodel_dev_read",
				"datamodel_prod_read",
			},
			readBuckets: []string{
				"ai-datasets",
				"biztech-edw-bronze",
				"biztech-edw-silver",
				"biztech-edw-gold",
				"biztech-edw-uat",
				"biztech-edw-dev",
				"data-pipelines-delta-lake-from-eu-west-1",
				"databricks-cloudtrail",
				"databricks-playground",
				"databricks-workspace",
				"fivetran-greenhouse-recruiting-delta-tables",
				"fivetran-netsuite-finance-delta-tables",
				"gtms",
				"labelbox",
				"netsuite-delta-tables",
				"netsuite-finance",
				"prod-app-configs",
				"spark-playground",
			},
			readDatabases: []string{
				"netsuite_data",
				"netsuite_suiteanalytics",
			},
			readwriteBuckets: []string{
				"amundsen-metadata",
			},
		},
		{
			clusterName:         "gql",
			instanceProfileName: "gql-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
			},
		},
		{
			clusterName:         "datascience-notebook-jobs",
			instanceProfileName: "datascience-notebook-jobs-cluster",
			readParameters:      standardReadParameters,
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readBuckets: standardDevAndSensitiveReadBuckets,
			readwriteBuckets: append(
				[]string{
					"benchmarking-metrics",
					"bigdata-datashare",
					"data-insights",
					"year-review-metrics",
				},
				sensitiveReadWriteBuckets...,
			),
			readwriteDatabases: []string{
				"datascience",
				"safety_map_data",
				"dataproducts",
				"stopsigns",
				"dojo",
				"dojo_tmp",
				"labelbox",
			},
			sqsProducerQueues: []string{
				"samsara_data_collection_create_internal_retrieval_*",
				"samsara_eu_data_collection_create_internal_retrieval_*",
			},
		},
		{
			clusterName:         "datascience-admin",
			instanceProfileName: "datascience-admin-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterSlackAppToken,
				dataplatformconsts.ParameterGoogleMapsApiToken,
				dataplatformconsts.ParameterGreenhouseApiToken,
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readBuckets: append(
				[]string{
					"databricks-workspace",
				},
				standardDevAndSensitiveReadBuckets...,
			),
			regionSpecificReadBuckets: map[string][]string{
				infraconsts.SamsaraAWSDefaultRegion: dataPipelineEUReplicatedBucketsForUSRegion,
			},
			readwriteBuckets: append(
				[]string{
					"benchmarking-metrics",
					"data-science-exploratory",
					"detailed-ifta-reports",
					"databricks-workspace/datascience",
				},
				sensitiveReadWriteBuckets...,
			),
			readwriteDatabases: []string{
				"api_logs",
				"customer360",
				"data_analytics",
				"databricks_alerts",
				"dataprep",
				"dataprep_cellular",
				"dataprep_safety",
				"dataprep_assets",
				"dataprep_firmware",
				"dataprep_ml",
				"dataprep_routing",
				"dataprep_telematics",
				"dataproducts",
				"datascience",
				"devexp",
				"dojo",
				"dojo_tmp",
				"fuel_maintenance",
				"ifta_report",
				"labelbox",
				"mixpanel",
				"mixpanel_samsara",
				"mobile_logs",
				"routeload_logs",
				"safety_map_data",
				"zendesk",
			},
			sqsProducerQueues: []string{
				"samsara_data_collection_create_internal_retrieval_*",
				"samsara_eu_data_collection_create_internal_retrieval_*",
			},
		},
		{
			clusterName:         "datastreams-admin",
			instanceProfileName: "datastreams-admin-cluster",
			policyAttachments: []string{
				"glue_catalog_admin",
				"s3_standard_dev_buckets_read",
			},
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
			readwriteBuckets: []string{
				"databricks-playground",
				"data-streams-delta-lake",
			},
			readBuckets: []string{
				"data-stream-lake",
			},
			readwriteDatabases: []string{
				"datastreams",
			},
		},
		{
			clusterName:         "delta-lake-deletion",
			instanceProfileName: "delta-lake-deletion-cluster",
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			readwriteBuckets: []string{
				"data-pipelines-delta-lake",
				"databricks-playground",
				"databricks-warehouse",
				"data-science-exploratory",
				"data-streams-delta-lake",
				"data-stream-lake",
				"kinesisstats-delta-lake",
				"report-staging-tables",
				"s3bigstats-delta-lake",
			},
		},
		{
			clusterName:         "deep-clone",
			instanceProfileName: "deep-clone-cluster",
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			readwriteBuckets: []string{
				"databricks-playground",
				"databricks-warehouse",
				"databricks-workspace",
			},
		},
		{
			clusterName:         "s3-inventory-delta",
			instanceProfileName: "s3-inventory-delta-cluster",
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			readBuckets: []string{
				"s3-inventory",
				"databricks-s3-inventory",
			},
			readwriteDatabases: []string{
				"s3inventories_delta",
			},
		},
		{
			clusterName:         "biztechenterprisedata-admin",
			instanceProfileName: "biztechenterprisedata-admin-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readBuckets: []string{
				"biztech-edw-silver",
				"databricks-netsuite-invoice-output",
				"data-eng-mapping-tables",
				"data-insights",
				"data-pipelines-delta-lake",
				"data-pipelines-delta-lake-from-eu-west-1",
				"data-stream-lake",
				"data-streams-delta-lake",
				"databricks-workspace",
				"fivetran-netsuite-finance-delta-tables",
				"kinesisstats-delta-lake",
				"mixpanel",
				"netsuite-delta-tables",
				"netsuite-invoice-exports",
				"partial-report-aggregation",
				"prod-app-configs",
				"rds-delta-lake",
				"report-staging-tables",
				"s3bigstats-delta-lake",
				"spark-playground",
				"year-review-metrics",
				"zendesk",
				"att-sftp",
			},
			readwriteBuckets: []string{
				"databricks-workspace/biztechenterprisedata",
				"netsuite-finance",
			},
			readwriteDatabases: []string{
				"kpatro_poc_biztech_edw_bronze",
				"kpatro_poc_biztech_edw_silver",
				"kpatro_poc_biztech_edw_gold",
				"dbt_poc_biztech_edw_bronze",
				"dbt_poc_biztech_edw_silver",
				"dbt_poc_biztech_edw_gold",
				"netsuite_data",
				"biztech_edw_customer_success_gold",
				"biztech_edw_dataquality_gold",
				"bigquery",
				"biztech_edw_dev",
				"biztech_edw_bronze",
				"biztech_edw_salesforce_bronze",
				"biztech_edw_uat",
				"connectedworker",
				"dataproducts",
				"supplychain",
				"supportops",
				"hardware",
			},
		},
		{
			clusterName:         "biztech-prod",
			instanceProfileName: "biztech-prod-cluster",
			policyAttachments: []string{
				"biztech_glue_database_read_write",
				"biztech_s3_bucket_read_write",
				"glue_catalog_readonly",
				"glue_playground_bucket_databases_write",
				"s3_standard_dev_buckets_read",
				"s3_standard_dev_buckets_write",
			},
			readDatabases: []string{
				"biztech_edw_dev",
				"biztech_edw_uat",
				"kpatro_poc_biztech_edw_bronze",
				"kpatro_poc_biztech_edw_silver",
				"kpatro_poc_biztech_edw_gold",
			},
			readBuckets: []string{
				"biztech-edw-silver",
				"databricks-netsuite-invoice-output",
				"data-pipelines-delta-lake-from-eu-west-1",
				"databricks-workspace",
				"fivetran-netsuite-finance-delta-tables",
				"netsuite-delta-tables",
				"netsuite-invoice-exports",
				"spark-playground",
				"netsuite-finance",
			},
			readwriteBuckets: []string{
				"netsuite-delta-tables",
				"netsuite-finance",
				"fivetran-netsuite-finance-delta-tables",
				"databricks-workspace/biztechenterprisedata",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				"netsuite_finance",
				"netsuite_suiteanalytics",
				"fivetran_netsuite_finance",
				"netsuite_suiteanalytics_for_connection_all_data",
				"netsuite_data",
				"biztech_edw_bronze",
				"biztech_edw_silver",
				"biztech_edw_customer_success_gold",
				"biztech_edw_dataquality_gold",
				"supplychain",
				"supportops",
				"dbt_poc_biztech_edw_bronze",
				"dbt_poc_biztech_edw_silver",
				"dbt_poc_biztech_edw_gold",
			),
		},
		{
			clusterName:         "biztech-dbt-dev",
			instanceProfileName: "biztech-dbt-dev-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"glue_playground_bucket_databases_write",
				"s3_standard_dev_buckets_read",
				"s3_standard_dev_buckets_write",
			},
			readDatabases: []string{
				"biztech_edw_sterling",
				"sda_gold",
				"biztech_edw_uat",
			},
			readBuckets: []string{
				"biztech-edw-bronze",
				"biztech-edw-silver",
				"biztech-edw-gold",
				"biztech-edw-uat",
				"netsuite-finance",
				"netsuite-delta-tables",
				"databricks-netsuite-invoice-output",
				"fivetran-netsuite-finance-delta-tables",
				"data-pipelines-delta-lake-from-eu-west-1",
				"netsuite-invoice-exports",
			},
			readwriteBuckets: []string{
				"databricks-workspace",
			},
			readwriteDatabases: []string{
				"kpatro_poc_biztech_edw_bronze",
				"kpatro_poc_biztech_edw_silver",
				"kpatro_poc_biztech_edw_gold",
				"biztech_edw_dev",
				"biztech_edw_salesforce_uat",
				"biztech_edw_netsuite_sb1",
				"biztech_edw_netsuite_sb2",
				"biztech_edw_netsuite2_sb1",
				"biztech_agautam",
				"biztech_galtobelli",
				"biztech_hvenkatesh",
				"biztech_isingh",
				"biztech_tlee",
				"biztech_klee",
				"biztech_kpatro",
				"biztech_mvaz",
				"biztech_ngadiraju",
				"biztech_pmuthusamy",
				"biztech_rvera",
				"biztech_spendry",
				"biztech_kheom",
				"biztech_tkelley",
				"biztech_sdas",
				"biztech_nmendoza",
				"biztech_ngurram",
				"parth_ucx", // temporarily add database for unity catalog evaluation
			},
			regionSpecificReadBuckets: map[string][]string{
				infraconsts.SamsaraAWSEURegion: {
					"samsara-biztech-edw-silver", // Grant the EU region access to the US bucket.
				},
			},
		},
		{
			clusterName:         "biztech-dbt-uat",
			instanceProfileName: "biztech-dbt-uat-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"glue_playground_bucket_databases_write",
				"s3_standard_dev_buckets_read",
				"s3_standard_dev_buckets_write",
			},
			readDatabases: []string{
				"biztech_edw_sterling",
			},
			readBuckets: []string{
				"biztech-edw-bronze",
				"biztech-edw-silver",
				"biztech-edw-gold",
				"biztech-sda-production",
				"netsuite-finance",
				"netsuite-delta-tables",
				"databricks-netsuite-invoice-output",
				"fivetran-netsuite-finance-delta-tables",
				"data-pipelines-delta-lake-from-eu-west-1",
				"netsuite-invoice-exports",
			},
			readwriteBuckets: []string{
				"databricks-workspace",
			},
			regionSpecificReadBuckets: map[string][]string{
				infraconsts.SamsaraAWSDefaultRegion: {
					"samsara-eu-biztech-edw-gold", // Grant the US region access to the EU bucket.
				},
				infraconsts.SamsaraAWSEURegion: {
					"samsara-biztech-edw-silver", // Grant the EU region access to the US bucket.
				},
			},
			readwriteDatabases: []string{
				"biztech_edw_uat",
				"biztech_edw_dev",
			},
		},
		{
			clusterName:         "dataplatform-dagster-prod",
			instanceProfileName: "dataplatform-dagster-prod-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterSlackAppToken,
				dataplatformconsts.ParameterGoogleMapsApiToken,
				dataplatformconsts.ParameterGreenhouseApiToken,
				dataplatformconsts.ParameterDagsterSlackBotToken,
				dataplatformconsts.ParameterDatabricksEuToken,
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
				dataplatformconsts.ParameterDatahubToken,
				dataplatformconsts.ParameterDatahubServer,
				dataplatformconsts.ParameterDatahubDbtApiToken,
				dataplatformconsts.ParameterDagsterDatabricksClientId,
				dataplatformconsts.ParameterDagsterDatabricksSecret,
				dataplatformconsts.ParameterDagsterDatabricksEuClientId,
				dataplatformconsts.ParameterDagsterDatabricksEuSecret,
				dataplatformconsts.ParameterDagsterDatabricksCaClientId,
				dataplatformconsts.ParameterDagsterDatabricksCaSecret,
			},
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
				"s3_standard_dev_buckets_write",
				"datamodel_dev_read",
				"datamodel_prod_read",
				"datamodel_prod_write",
				"metrics_repo_prod_write",
			},
			readBuckets: append([]string{
				"bigdata-datashare",
				"datasets",
				"workforce-data",
				"databricks-workspace",
				"biztech-edw-sterling",
			},
				sensitiveReadBuckets...,
			),
			regionSpecificReadBuckets: map[string][]string{
				// Grant the US access to EU/CA data.
				infraconsts.SamsaraAWSDefaultRegion: append(
					dataPipelineEUReplicatedBucketsForUSRegion,
					"samsara-eu-datamodel-warehouse-dev",
					"samsara-eu-datamodel-warehouse",
					"samsara-eu-rds-delta-lake",
					"samsara-eu-data-streams-delta-lake",
					"samsara-eu-kinesisstats-delta-lake",
					"samsara-eu-data-pipelines-delta-lake",
					"samsara-eu-data-stream-lake",
					"samsara-eu-dynamodb-delta-lake",
					"samsara-eu-s3bigstats-delta-lake",
					"samsara-ca-datamodel-warehouse",
					"samsara-ca-rds-delta-lake",
					"samsara-ca-data-streams-delta-lake",
					"samsara-ca-kinesisstats-delta-lake",
					"samsara-ca-data-pipelines-delta-lake",
					"samsara-ca-data-stream-lake",
					"samsara-ca-dynamodb-delta-lake",
					"samsara-ca-s3bigstats-delta-lake",
				),
				// Grant the EU access to a US bucket.
				infraconsts.SamsaraAWSEURegion: {
					"samsara-biztech-edw-silver",
					"samsara-datamodel-warehouse",
					"samsara-databricks-warehouse",
				},
			},
			readwriteBuckets: []string{ // lint: +sorted
				"amundsen-metadata",
				"benchmarking-metrics",
				"data-science-exploratory",
				"detailed-ifta-reports",
			},
			readDatabases: []string{"biztech_edw_silver", "biztech_edw_customer_success_gold"},
			readwriteDatabases: []string{
				"api_logs",
				"auditlog",
				"customer360",
				"canonical_distance_updates",
				"data_analytics",
				"databricks_alerts",
				"dataengineering",
				"dataengineering_dev",
				"dataplatform_dev",
				"dataprep",
				"dataprep_cellular",
				"dataprep_safety",
				"dataprep_assets",
				"dataprep_firmware",
				"dataprep_ml",
				"dataprep_routing",
				"dataprep_telematics",
				"dataproducts",
				"datascience",
				"devexp",
				"firmware",
				"firmware_dev",
				"fuel_maintenance",
				"ifta_report",
				"mixpanel",
				"mixpanel_samsara",
				"mobile_logs",
				"routeload_logs",
				"zendesk",
			},
		},
		{
			clusterName:         "dataplatform-dagster-dev",
			instanceProfileName: "dataplatform-dagster-dev-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterSlackAppToken,
				dataplatformconsts.ParameterGoogleMapsApiToken,
				dataplatformconsts.ParameterGreenhouseApiToken,
				dataplatformconsts.ParameterDagsterSlackBotToken,
				dataplatformconsts.ParameterDatabricksEuToken,
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
				dataplatformconsts.ParameterDatahubToken,
				dataplatformconsts.ParameterDatahubServer,
				dataplatformconsts.ParameterDatahubDbtApiToken,
			},
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
				"s3_standard_dev_buckets_write",
				"datamodel_dev_read",
				"datamodel_dev_write",
				"datamodel_prod_read",
				"metrics_repo_dev_write",
			},
			readBuckets: append([]string{
				"bigdata-datashare",
				"datasets",
				"workforce-data",
				"databricks-workspace",
				"biztech-edw-sterling",
			},
				sensitiveReadBuckets...,
			),
			regionSpecificReadBuckets: map[string][]string{
				// Grant the US access to EU/CA data.
				infraconsts.SamsaraAWSDefaultRegion: append(
					dataPipelineEUReplicatedBucketsForUSRegion,
					"samsara-eu-datamodel-warehouse-dev",
					"samsara-eu-datamodel-warehouse",
					"samsara-eu-rds-delta-lake",
					"samsara-eu-data-streams-delta-lake",
					"samsara-eu-kinesisstats-delta-lake",
					"samsara-eu-data-pipelines-delta-lake",
					"samsara-eu-data-stream-lake",
					"samsara-eu-dynamodb-delta-lake",
					"samsara-eu-s3bigstats-delta-lake",
					"samsara-ca-datamodel-warehouse",
					"samsara-ca-rds-delta-lake",
					"samsara-ca-data-streams-delta-lake",
					"samsara-ca-kinesisstats-delta-lake",
					"samsara-ca-data-pipelines-delta-lake",
					"samsara-ca-data-stream-lake",
					"samsara-ca-dynamodb-delta-lake",
					"samsara-ca-s3bigstats-delta-lake",
				),
				// Grant the EU access to a US bucket.
				infraconsts.SamsaraAWSEURegion: {
					"samsara-biztech-edw-silver",
					"samsara-datamodel-warehouse",
					"samsara-databricks-warehouse",
				},
			},
			readwriteBuckets: []string{ // lint: +sorted
				"amundsen-metadata",
				"benchmarking-metrics",
				"data-science-exploratory",
				"detailed-ifta-reports",
			},
			readDatabases: []string{"biztech_edw_silver", "biztech_edw_customer_success_gold"},
			readwriteDatabases: []string{
				"api_logs",
				"customer360",
				"canonical_distance_updates",
				"data_analytics",
				"databricks_alerts",
				"dataengineering",
				"dataengineering_dev",
				"dataplatform_dev",
				"dataprep",
				"dataprep_cellular",
				"dataprep_safety",
				"dataprep_assets",
				"dataprep_firmware",
				"dataprep_ml",
				"dataprep_routing",
				"dataprep_telematics",
				"dataproducts",
				"datascience",
				"devexp",
				"firmware",
				"fuel_maintenance",
				"ifta_report",
				"mixpanel",
				"mixpanel_samsara",
				"mobile_logs",
				"routeload_logs",
				"zendesk",
			},
		},
		{
			clusterName:         "firmware-rollout-stage-rebalance",
			instanceProfileName: "firmware-rollout-stage-rebalance-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readBuckets: []string{
				"data-eng-mapping-tables",
				"data-pipelines-delta-lake",
				"databricks-workspace",
				"rds-delta-lake",
			},
			readwriteBuckets: []string{ // lint: +sorted
				"benchmarking-metrics",
				"databricks-playground",
				"databricks-warehouse",
				"databricks-workspace/" + strings.ToLower(team.Firmware.TeamName),
			},
			readDatabases: []string{
				"customer360",
			},
			readwriteDatabases: []string{
				"firmware_dev",
			},
		},
		{
			clusterName:         "opx-fivetran-incidentio",
			instanceProfileName: "opx-fivetran-incidentio-cluster",
			canCreateDatabases:  true,
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				// This DB is created by the netsuite connector in fivetran to sync the tables
				"incidentio_bronze",
			),
			readwriteBuckets: []string{
				"fivetran-incidentio-delta-tables",
				// where Fivetran syncs data to
				"databricks-warehouse",
				"databricks-playground", // _fivetran_setup_test is set up under the playground bucket for some reason
			},
		},
		{
			clusterName:         "opx-fivetran-pagerduty",
			instanceProfileName: "opx-fivetran-pagerduty-cluster",
			canCreateDatabases:  true,
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				// This DB is created by the netsuite connector in fivetran to sync the tables
				"pagerduty_bronze",
			),
			readwriteBuckets: []string{
				"fivetran-pagerduty-delta-tables",
				// where Fivetran syncs data to
				"databricks-warehouse",
				"databricks-playground", // _fivetran_setup_test is set up under the playground bucket for some reason
			},
		},
		{
			clusterName:         "opx-fivetran-launchdarkly",
			instanceProfileName: "opx-fivetran-launchdarkly-cluster",
			canCreateDatabases:  true,
			policyAttachments: []string{
				"glue_catalog_readonly",
			},
			readwriteDatabases: append(
				fivetranReadWriteDatabases,
				// This DB is created by the netsuite connector in fivetran to sync the tables
				"launchdarkly_bronze",
			),
			readwriteBuckets: []string{
				"fivetran-launchdarkly-delta-tables",
				// where Fivetran syncs data to
				"databricks-warehouse",
				"databricks-playground", // _fivetran_setup_test is set up under the playground bucket for some reason
			},
		},
		{
			clusterName:         "fleetsecurity",
			instanceProfileName: "fleetsecurity-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
				"s3_standard_dev_buckets_write",
			},
			readBuckets: standardDevReadBuckets,
			readwriteBuckets: []string{
				"databricks-playground",
				"databricks-workspace",
			},
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogAppToken,
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatabricksEuToken,
			},
			readwriteDatabases: []string{
				"fleetsec_dev",
			},
		},
		{
			clusterName:         "platformalertsworkflows-processor",
			instanceProfileName: "platformalertsworkflows-processor-cluster",
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
				"s3_standard_dev_buckets_write",
			},
			readBuckets: standardDevReadBuckets,
			readwriteBuckets: []string{
				"databricks-playground",
				"databricks-workspace",
			},
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogAppToken,
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatabricksEuToken,
			},
		},
	}
}

func GetUnityCatalogClusterProfiles(config dataplatformconfig.DatabricksConfig) []ClusterProfile {
	return []ClusterProfile{
		{
			clusterName:         "dynamodb-import-uc",
			instanceProfileName: "dynamodb-import-uc-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			readBuckets: []string{
				"dynamodb-export",
			},
			readwriteBuckets: []string{
				"dynamodb-delta-lake",
			},
		},
		{
			clusterName:         "s3bigstats-import-uc",
			instanceProfileName: "s3bigstats-import-uc-cluster",
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			sqsConsumerQueues: []string{
				"samsara_delta_lake_merge_s3_big_stat_*",
			},
			readBuckets: []string{
				"s3bigstats-json-export",
				"kinesisstats-delta-lake",
			},
			readwriteBuckets: []string{
				"databricks-warehouse",
				"s3bigstats-delta-lake",
			},
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
		},
		{
			clusterName:         "kinesisstats-import-uc",
			instanceProfileName: "kinesisstats-import-uc-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			sqsConsumerQueues: []string{
				"samsara_delta_lake_merge_ks_*",
			},
			readBuckets: []string{
				"kinesisstats-json-export",
			},
			readwriteBuckets: []string{
				"kinesisstats-delta-lake",
				"databricks-job-resources",
				"databricks-workspace",
			},
			readwriteDatabases: []string{
				"auditlog",
			},
		},
		{
			clusterName:         "rds-import-uc",
			instanceProfileName: "rds-import-uc-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			sqsConsumerQueues: []string{
				"samsara_delta_lake_rds_*",
			},
			readBuckets: []string{
				"rds-export",
				"prod-app-configs",
			},
			readwriteBuckets: []string{
				"rds-delta-lake",
			},
			readwriteDatabases: []string{
				"auditlog",
			},
		},
		{
			clusterName:         "datastreams-admin-uc",
			instanceProfileName: "datastreams-admin-uc-cluster",
			policyAttachments: []string{
				"glue_catalog_admin",
			},
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterDatadogAppToken,
			},
			readBuckets: []string{
				"data-stream-lake",
			},
			readwriteBuckets: []string{
				"data-streams-delta-lake",
			},
			readwriteDatabases: []string{
				"datastreams",
			},
		},
		{
			clusterName:         "dev-uc",
			instanceProfileName: "dev-uc-cluster",
			readParameters: []dataplatformconsts.Parameter{
				dataplatformconsts.ParameterSlackAppToken,
				dataplatformconsts.ParameterDatadogAppToken,
				dataplatformconsts.ParameterDatadogApiToken,
				dataplatformconsts.ParameterGoogleMapsApiToken,
			},
			policyAttachments: []string{
				"glue_catalog_readonly",
				"s3_standard_dev_buckets_read",
			},
			readBuckets: []string{
				"spark-playground",
			},
			regionSpecificReadBuckets: map[string][]string{
				infraconsts.SamsaraAWSDefaultRegion: dataPipelineEUReplicatedBucketsForUSRegion,
			},
			readwriteDatabases: getPlaygroundBucketDbNames(config),
		},
		{
			clusterName:         "unity-catalog",
			instanceProfileName: "unity-catalog-cluster",
		},
	}
}

func allRawClusterProfiles(config dataplatformconfig.DatabricksConfig) []ClusterProfile {
	var allConfigs []ClusterProfile
	allConfigs = append(allConfigs, getCustomClusterProfiles(config)...)
	for _, cluster := range GetUnityCatalogClusterProfiles(config) {
		cluster.isForUnityCatalog = true
		allConfigs = append(allConfigs, cluster)
	}
	for _, t := range clusterhelpers.DatabricksClusterTeams() {
		allConfigs = append(allConfigs, makeTeamClusterProfile(&t, config))
	}
	return allConfigs
}

// allUpdatedClusterProfiles returns cluster profiles after all overrides have been made, and bucket permissions have been added.
func allUpdatedClusterProfiles(c dataplatformconfig.DatabricksConfig, region string) ([]ClusterProfile, error) {
	// Update biztech-prod and biztechenterprisedata readwrite databases to only be the unique ones that do not overlap.
	// The overlap of these DBs gets extracted into a managed policy (to save on inline policy space) and added to both of these profiles.
	biztechProdReadWriteDbs, biztechenterprisedataReadWriteDbs, _, err := findIntersectionOfBiztechenterprisedataAndBiztechProdReadWriteDbs(c)
	if err != nil {
		return nil, oops.Wrapf(err, "Error finding biztech-prod and biztechenterprisedata readwriteDatabases intersection")
	}
	prefix := awsregionconsts.RegionPrefix[region]
	var updatedConfigs []ClusterProfile
	for _, config := range allRawClusterProfiles(c) {
		if config.clusterName == "biztech-prod" {
			config.readwriteDatabases = biztechProdReadWriteDbs
		} else if config.clusterName == strings.ToLower(team.BizTechEnterpriseData.TeamName) {
			config.readwriteDatabases = biztechenterprisedataReadWriteDbs
		}

		// Add the required readwrite bucket permissions for the database lists.
		readWriteBucketsList := databaseregistries.GetBucketsForDatabasesInRegion(c, config.readwriteDatabases, databaseregistries.ReadWrite, databaseregistries.LegacyOnlyDatabases)
		if regionReadWriteBuckets, ok := config.regionSpecificReadWriteBuckets[region]; ok {
			readWriteBucketsList = append(readWriteBucketsList, regionReadWriteBuckets...)
		}
		for _, bucket := range config.readwriteBuckets {
			readWriteBucketsList = append(readWriteBucketsList, prefix+bucket)
		}
		config.readwriteBuckets = uniqueElements(readWriteBucketsList)

		// Add the required read bucket permissions for the database lists.
		readBucketsList := databaseregistries.GetBucketsForDatabasesInRegion(c, config.readDatabases, databaseregistries.Read, databaseregistries.LegacyOnlyDatabases)
		if regionReadBuckets, ok := config.regionSpecificReadBuckets[region]; ok {
			readBucketsList = append(readBucketsList, regionReadBuckets...)
		}
		for _, bucket := range config.readBuckets {
			readBucketsList = append(readBucketsList, prefix+bucket)
		}
		// Filter out buckets that already appear in the readWriteBuckets list, because they already have read access.
		var filteredReadBucketsList []string
		readWriteBucketLookup := listToMap(readWriteBucketsList)
		for _, bucket := range readBucketsList {
			if _, ok := readWriteBucketLookup[bucket]; !ok {
				filteredReadBucketsList = append(filteredReadBucketsList, bucket)
			}
		}
		config.readBuckets = uniqueElements(filteredReadBucketsList)

		updatedConfigs = append(updatedConfigs, config)
	}
	return updatedConfigs, nil
}

func makeTeamClusterProfile(teamInfo *components.TeamInfo, c dataplatformconfig.DatabricksConfig) ClusterProfile {
	// Get the team specific overrides, if any, and apply them.
	overrides := teamClusterOverride{}
	if entry, ok := teamClusterOverrides[teamInfo.TeamName]; ok {
		overrides = entry
	}

	var readBuckets []string
	readBuckets = append(readBuckets, "spark-playground")
	readBuckets = append(readBuckets, "databricks-workspace")
	readBuckets = append(readBuckets, overrides.readBuckets...)

	var readWriteBuckets []string
	readWriteBuckets = append(readWriteBuckets, overrides.readWriteBuckets...)

	var regionSpecificReadWriteBuckets = make(map[string][]string)
	for region, buckets := range overrides.regionSpecificReadWriteBuckets {
		regionSpecificReadWriteBuckets[region] = buckets
	}

	var regionSpecificReadBuckets = make(map[string][]string)
	for region, buckets := range overrides.regionSpecificReadBuckets {
		regionSpecificReadBuckets[region] = buckets
	}
	// Add this data pipelines related bucket so that US env can read data pipeline data copied from EU to US
	regionSpecificReadBuckets[infraconsts.SamsaraAWSDefaultRegion] = append(
		regionSpecificReadBuckets[infraconsts.SamsaraAWSDefaultRegion], dataPipelineEUReplicatedBucketsForUSRegion...)

	// Allow each team read/write access to their own team workspace bucket
	readWriteBuckets = append(readWriteBuckets, fmt.Sprintf("databricks-workspace/%s", strings.ToLower(teamInfo.TeamName)))

	var readWriteDatabases []string
	readWriteDatabases = append(readWriteDatabases, overrides.readWriteDatabases...)

	// Give team read/write privs to their own DB and read privs for the rest
	for _, db := range databaseregistries.GetDatabasesByGroup(c, databaseregistries.TeamDevDatabaseGroup, databaseregistries.LegacyOnlyDatabases) {
		if teamInfo.TeamName == db.OwnerTeam.TeamName {
			// Bucket access will automatically be granted by adding the DB.
			readWriteDatabases = append(readWriteDatabases, strings.ToLower(db.OwnerTeam.TeamName)+"_dev")
		} else {
			readBuckets = append(readBuckets, fmt.Sprintf("databricks-workspace/team_dbs/%s_dev.db", strings.ToLower(teamInfo.TeamName)))
		}
	}

	var readParameters []dataplatformconsts.Parameter
	readParameters = append(readParameters, standardReadParameters...)
	readParameters = append(readParameters, overrides.readParameters...)

	var sqsProducerQueues []string
	sqsProducerQueues = append(sqsProducerQueues, overrides.sqsProducerQueues...)

	var policyAttachments = append([]string{
		"dev_team_glue_database_read",
		"glue_catalog_readonly",
		"glue_playground_bucket_databases_write",
		"s3_standard_dev_buckets_read",
		"s3_standard_dev_buckets_write",
	}, overrides.policyAttachments...)

	return ClusterProfile{
		instanceProfileName:            strings.ToLower(teamInfo.TeamName) + "-cluster",
		clusterName:                    strings.ToLower(teamInfo.TeamName),
		readParameters:                 readParameters,
		policyAttachments:              policyAttachments,
		readBuckets:                    readBuckets,
		regionSpecificReadWriteBuckets: regionSpecificReadWriteBuckets,
		regionSpecificReadBuckets:      regionSpecificReadBuckets,
		readwriteBuckets:               readWriteBuckets,
		readwriteDatabases:             readWriteDatabases,
		readDatabases:                  overrides.readDatabases,
		sqsProducerQueues:              sqsProducerQueues,
		teamReadWriteDatabases:         overrides.readWriteDatabases,
	}
}

func instanceProfileName(clusterName string) string {
	return clusterName + "-cluster"
}

/**
 * Team Cluster Profile Overrides
 * Configure extra buckets and permissions that your team needs here.
 */
type teamClusterOverride struct {
	readBuckets                    []string
	readWriteBuckets               []string
	regionSpecificReadWriteBuckets map[string][]string
	regionSpecificReadBuckets      map[string][]string
	readWriteDatabases             []string
	readDatabases                  []string
	readParameters                 []dataplatformconsts.Parameter
	sqsProducerQueues              []string
	policyAttachments              []string
}

var sensitiveReadBuckets = []string{ // lint: +sorted
	"cvdata",
	"dashcam-videos",
	"ml-models",
	"workforce-video-assets",
}

var standardDevAndSensitiveReadBuckets = append(
	sensitiveReadBuckets,
	standardDevReadBuckets...,
)

var sensitiveReadWriteBuckets = []string{ // lint: +sorted
	"bigdata-datashare",
	"datasets",
	"workforce-data",
}

var teamClusterOverrides = map[string]teamClusterOverride{
	// BusinessSystemsRecruiting cluster uses `fivetran-greenhouse-recruiting-cluster`
	// as its instance profile so adding to this cluster IAM role will not give the
	// team using the cluster the proper access.
	// TODO: determine if we should point the cluster to the team's cluster profile
	// instead of `fivetran-greenhouse-recruiting-cluster`.
	team.AssetsFirmware.TeamName: {
		readWriteDatabases: []string{
			"firmware",
		},
	},
	team.BusinessSystemsRecruiting.TeamName: {
		readWriteDatabases: []string{
			"shared_people_ops_data",
			"supportops",
		},
	},
	team.Cloud.TeamName: {
		readWriteBuckets: []string{
			"gql-mapping",
		},
		readDatabases: []string{
			"perf_infra",
		},
	},
	team.ConnectedWorker.TeamName: {
		readBuckets:      sensitiveReadBuckets,
		readWriteBuckets: sensitiveReadWriteBuckets,
		readDatabases: []string{
			"dojo",
		},
	},
	team.DataAnalytics.TeamName: {
		policyAttachments: []string{
			"datamodel_dev_read",
			"datamodel_prod_read",
		},
		readWriteBuckets: []string{
			"benchmarking-metrics",
			"data-insights",
			"year-review-metrics",
		},
		readWriteDatabases: []string{
			"data_analytics",
			"shared_people_ops_data",
			"finops",
			"datamodel_dev",
			"auditlog",
			"dataplatform_dev",
		},
		readDatabases: []string{
			"biztech_edw_silver",
			"biztech_edw_uat",
			"dojo",
			"labelbox",
			"safety_map_data",
		},
	},
	team.DataEngineering.TeamName: {
		policyAttachments: []string{
			"datamodel_dev_read",
			"datamodel_prod_read",
		},
		readBuckets: append([]string{
			"databricks-kinesisstats-diffs",
			"ai-datasets",
		}, sensitiveReadBuckets...),
		readWriteBuckets: []string{
			"bigdata-datashare",
			"databricks-playground",
			"datasets",
			"year-review-metrics",
		},
		readDatabases: []string{
			"biztech_edw_silver",
			"biztech_edw_uat",
			"dojo",
			"safety_map_data",
			"labelbox",
		},
		readWriteDatabases: []string{
			"dataprep",
			"dataprep_safety",
			"dataengineering",
			"year_review_metrics",
			"datamodel_dev",
			"mixpanel_samsara",
		},
	},
	team.DataScience.TeamName: {
		policyAttachments: []string{
			"datamodel_dev_read",
			"datamodel_prod_read",
		},
		readBuckets: sensitiveReadBuckets,
		readWriteBuckets: append([]string{
			"benchmarking-metrics",
			"bigdata-datashare",
			"data-insights",
			"dojo-data-exchange",
			"year-review-metrics",
		}, sensitiveReadWriteBuckets...),
		readWriteDatabases: []string{
			"safety_map_data",
			"dojo",
			"dojo_tmp",
			"labelbox",
		},
		readParameters: standardReadParameters,
	},
	team.DataTools.TeamName: {
		readWriteDatabases: []string{
			"dataplatform_dev",
			"auditlog",
			"mixpanel_samsara",
		},
	},
	team.DataPlatform.TeamName: {
		regionSpecificReadBuckets: map[string][]string{
			// In the US, we have fivetran data within this bucket.
			infraconsts.SamsaraAWSDefaultRegion: {
				"samsara-dataplatform-fivetran-bronze",
			},
		},
		readWriteBuckets: []string{
			"databricks-kinesisstats-diffs",
			"databricks-rds-diffs",
			"year-review-metrics",
		},
		readWriteDatabases: []string{
			"api_logs", // To write to api_logs.api_logs_delta_table.
			"billing",
			"year_review_metrics",
			"dataplat_ucx",
			"dataplatform_dev",
			"auditlog",
		},
	},
	team.DevEcosystem.TeamName: {
		readWriteDatabases: []string{
			"devecosystem",
		},
	},
	team.EpoFinance.TeamName: {
		readWriteDatabases: []string{
			"epofinance",
			"epofinance_prod",
		},
		readWriteBuckets: []string{
			"databricks-warehouse",
			"epofinance",
		},
	},
	team.FinancialOperations.TeamName: {
		readWriteBuckets: []string{
			"netsuite-finance",
		},
		readBuckets: []string{
			"netsuite-delta-tables",
			"biztech-edw-gold",
			"biztech-edw-uat",
		},
		readWriteDatabases: []string{
			"finops",
		},
		readDatabases: []string{
			"fivetran_netsuite_finance",
			"netsuite_suiteanalytics",
			"netsuite_data",
			"netsuite_finance",
			"biztech_edw_uat",
			"biztech_edw_silver",
		},
	},
	team.MLInfra.TeamName: {
		policyAttachments: []string{
			"ml_feature_platform_policy",
			"ml_code_artifact_read_policy",
		},
	},
	team.ProductManagement.TeamName: {
		readDatabases: []string{
			"biztech_edw_silver",
		},
	},
	team.ReleaseManagement.TeamName: {
		readWriteDatabases: []string{
			"release_management",
		},
	},
	team.Support.TeamName: {
		readBuckets: []string{
			"support-cloudwatch-logs-bucket",
		},
		readWriteDatabases: []string{
			"supportops",
			"technical_support",
		},
		readDatabases: []string{
			"biztech_edw_silver",
			"biztech_edw_uat",
		},
	},
	team.EngineeringOperations.TeamName: {
		readWriteDatabases: []string{
			"finops",
		},
	},
	team.ConnectedEquipment.TeamName: {
		readWriteBuckets: []string{
			"network-coverage",
		},
		readDatabases: []string{
			"biztech_edw_silver",
		},
	},
	team.STCEUnpowered.TeamName: {
		readWriteBuckets: []string{
			"network-coverage",
			"smart-maps",
		},
	},
	team.SupportOps.TeamName: {
		readBuckets: []string{
			"biztech-edw-silver",
		},
		readDatabases: []string{
			"biztech_edw_silver",
		},
	},
	team.Firmware.TeamName: {
		readBuckets: []string{
			"dashcam-videos",
			"sensor-replay",
			"ai-datasets",
		},
		readDatabases: []string{
			"dojo",
		},
		readWriteDatabases: []string{
			"firmware",
		},
	},
	// Generally we want to keep the vdp cluster in sync with the firmware cluster,
	// as the teams operate very similarly.
	team.FirmwareVdp.TeamName: {
		readBuckets: []string{
			"dashcam-videos",
			"sensor-replay",
			"ai-datasets",
			"safety-map-data-sources",
		},
		readDatabases: []string{
			"dojo",
		},
		readWriteDatabases: []string{
			"firmware",
			"firmware_dev", // Many VDP members use the firmware_dev database too.
			"datamodel_dev",
			"dataplatform_dev",
			"auditlog",
		},
	},
	team.FuelServices.TeamName: {
		readWriteBuckets: []string{
			"benchmarking-metrics",
		},
	},
	team.Sustainability.TeamName: {
		readWriteDatabases: []string{
			"evsandecodriving_dev",
		},
		readWriteBuckets: []string{
			"benchmarking-metrics",
			// The evs and ecodriving team took over fuelservices code which writes
			// to this location and needs s3 permissions.
			"databricks-workspace/fuelservices",
		},
	},
	team.MediaExperience.TeamName: {
		readWriteDatabases: []string{
			"mediaexperience_prod",
		},
		readWriteBuckets: []string{
			"mediaexperience_prod",
		},
	},
	team.GTMS.TeamName: {
		readWriteBuckets: []string{
			"gtms",
		},
	},
	team.DataProducts.TeamName: {
		readWriteBuckets: []string{
			"benchmarking-metrics",
		},
	},
	team.Routing.TeamName: {
		readWriteDatabases: []string{
			"dataprep_routing",
		},
		readDatabases: []string{
			"safety_map_data",
		},
		readWriteBuckets: []string{
			"safety-map-data-sources",
		},
		readParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterHereMapsApiToken,
		},
	},
	team.Maps.TeamName: {
		readDatabases: []string{
			"safety_map_data",
		},
		readWriteBuckets: []string{
			"safety-map-data-sources",
		},
	},
	team.Hardware.TeamName: {
		readWriteDatabases: []string{
			"hardware",
			"hardware_analytics",
		},
		readBuckets: []string{
			"firmware-test-automation",
		},
		readDatabases: []string{
			"biztech_edw_silver",
		},
	},
	team.Oem.TeamName: {
		readWriteDatabases: []string{
			"dataprep_telematics",
		},
	},
	team.PlatformAlertsWorkflows.TeamName: {
		readBuckets: standardDevReadBuckets,

		readWriteBuckets: []string{
			"databricks-playground",
			"databricks-workspace",
		},
	},
	team.PlatformOperations.TeamName: {
		sqsProducerQueues: []string{
			"samsara_netsuiteinvoiceworker_*",
			"samsara_eu_netsuiteinvoiceworker_*",
			"samsara_licenseingestionworker_*",
			"samsara_eu_licenseingestionworker_*",
		},
		readBuckets: []string{
			"netsuite-delta-tables",
			"netsuite-sam-reports",
			"databricks-netsuite-invoice-output",
			"platops-databricks-metadata",
		},
		readWriteDatabases: []string{
			"netsuite_data",
			"platops",
			"sfdc_data",
		},
		readDatabases: []string{
			"biztech_edw_silver",
		},
	},
	team.PlatformReports.TeamName: {
		readWriteBuckets: []string{
			"year-review-metrics",
		},
	},
	team.BusinessSystems.TeamName: {
		readBuckets: []string{
			"netsuite-delta-tables",
			"databricks-netsuite-invoice-output",
			"netsuite-invoice-exports",
		},
	},
	team.SafetyFirmware.TeamName: {
		readBuckets: append([]string{
			"ai-datasets",
		}, sensitiveReadBuckets...),
		readWriteBuckets: append([]string{"bigdata-datashare"}, sensitiveReadWriteBuckets...),
		readDatabases: []string{
			"dojo",
			"safety_map_data",
		},
	},
	team.SafetyADAS.TeamName: {
		readBuckets:      sensitiveReadBuckets,
		readWriteBuckets: sensitiveReadWriteBuckets,
	},
	team.SafetyCameraServices.TeamName: {
		readBuckets: sensitiveReadBuckets,
	},
	team.SafetyPlatform.TeamName: {
		readBuckets: append([]string{
			"ai-datasets",
		}, sensitiveReadBuckets...),
		readWriteBuckets: append([]string{
			"map-tiles",
		}, sensitiveReadWriteBuckets...),
		readDatabases: []string{
			"dojo",
			"dojo_tmp",
		},
		readWriteDatabases: []string{
			"dataprep_safety",
			"dataprep_routing",
			"safety_map_data",
		},
		readParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterTomTomApiToken,
			dataplatformconsts.ParameterHereMapsApiToken,
		},
	},
	team.SafetyEventIngestionAndFiltering.TeamName: {
		readBuckets: sensitiveReadBuckets,
		readWriteBuckets: append([]string{
			"map-tiles",
		}, sensitiveReadWriteBuckets...),
		readWriteDatabases: []string{
			"safety_map_data",
		},
		readDatabases: []string{
			"dojo",
			"dojo_tmp",
		},
		readParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterTomTomApiToken,
		},
	},
	team.SalesEngineering.TeamName: {
		readWriteDatabases: []string{
			"solutions_integration_services",
		},
		readDatabases: []string{
			"biztech_edw_silver",
		},
	},
	team.SRE.TeamName: {
		readBuckets: []string{
			"ec2-logs",
			"aws-reports",
			"org-wide-storage-lens-export",
		},
		readWriteDatabases: []string{
			"perf_infra",
		},
	},
	team.SupplyChain.TeamName: {
		readWriteBuckets: []string{
			"biztech-edw-gold",
			"biztech-edw-bronze",
		},
		readDatabases: []string{
			"biztech_edw_silver",
		},
		readWriteDatabases: []string{
			"supplychain",
		},
	},
	team.CoreServices.TeamName: {
		readBuckets: []string{
			"prod-app-logs",
			"vodafone-reports",
		},
		readWriteBuckets: []string{
			"thor-variants",
		},
		readParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterJdmFtpSshPrivateKey,
		},
	},
	team.Security.TeamName: {
		readBuckets: []string{
			"load-balancer-access-logs",
		},
	},
	team.MLCV.TeamName: {
		readBuckets: sensitiveReadBuckets,
		readWriteBuckets: append([]string{
			"dojo-data-exchange",
		}, sensitiveReadWriteBuckets...),
		sqsProducerQueues: []string{
			"samsara_data_collection_create_internal_retrieval_*",
			"samsara_eu_data_collection_create_internal_retrieval_*",
		},
		readWriteDatabases: []string{
			"dojo",
			"dojo_tmp",
			"labelbox",
		},
	},
	team.BizTechEnterpriseData.TeamName: {
		policyAttachments: []string{
			"biztech_glue_database_read_write",
			"biztech_s3_bucket_read_write",
		},
		readWriteDatabases: []string{
			"biztech_edw_netsuite_bronze",
			"biztech_edw_customer_success_gold",
			"biztech_edw_silver",
			"biztech_edw_dev",
			"biztech_edw_uat",
			"supplychain",
			"netsuite_data",
			"hardware",
		},
		readBuckets: []string{
			"biztech-sda-production",
			"netsuite-delta-tables",
			"databricks-netsuite-invoice-output",
			"netsuite-invoice-exports",
		},
		readDatabases: []string{
			"biztech_edw_sterling",
			"fivetran_netsuite_finance",
			"netsuite_suiteanalytics",
			"sda_gold",
		},
	},
	team.BizTechEnterpriseDataSensitive.TeamName: {
		readBuckets: []string{
			"biztech-edw-silver",
			"biztech-edw-gold",
		},
		readWriteDatabases: append(
			fivetranReadWriteDatabases,
			"biztech_edw_greenhouse_bronze",
			"fivetran_backspace_compatible_staging",
		),
	},
	team.BizTechEnterpriseDataApplication.TeamName: {
		readBuckets: []string{
			"biztech-edw-dev",
			"biztech-edw-uat",
		},
		readWriteBuckets: []string{
			"biztech-edw-bronze",
			"biztech-edw-gold",
		},
		readWriteDatabases: []string{
			"biztech_edw_silver",
			"biztech_edw_sterling",
		},
	},
	team.CustomerSuccessOperations.TeamName: {
		policyAttachments: []string{
			"datamodel_prod_read",
		},
		readBuckets: []string{
			"biztech-edw-silver",
		},
		readDatabases: []string{
			"biztech_edw_silver",
			"biztech_edw_customer_success_gold",
		},
		readWriteDatabases: []string{
			"biztech_njaton",
		},
	},
	team.SeOps.TeamName: {
		readWriteBuckets: []string{
			"biztech-edw-gold",
		},
		readDatabases: []string{
			"biztech_edw_silver",
		},
	},
	team.TPM.TeamName: {
		readDatabases: []string{
			"biztech_edw_silver",
		},
	},
	team.SalesDataAnalytics.TeamName: {
		readWriteBuckets: []string{
			"biztech-sda-production",
		},
		readWriteDatabases: []string{
			"biztech_yagrawal",
			"sda_gold",
		},
		readDatabases: []string{
			"biztech_edw_silver",
		},
	},
	team.DecisionScience.TeamName: {
		policyAttachments: []string{
			"datamodel_dev_read",
			"datamodel_prod_read",
		},
		readDatabases: []string{
			"biztech_edw_silver",
		},
		readBuckets: []string{
			"biztech-edw-silver",
		},
	},
	team.Compliance.TeamName: {
		readWriteDatabases: []string{
			"compliance",
		},
		readParameters: []dataplatformconsts.Parameter{
			dataplatformconsts.ParameterDMV2RolloutLDKey,
		},
	},
	team.QualityAssured.TeamName: {
		readBuckets: []string{
			"firmware-test-automation",
		},
	},
	team.TestAutomation.TeamName: {
		readBuckets: []string{
			"firmware-test-automation",
		},
		readWriteDatabases: []string{
			"owltomation_results",
		},
	},
	team.SmartMaps.TeamName: {
		readWriteBuckets: []string{"smart-maps"},
		readBuckets: []string{
			"smart-maps",
			"dashcam-videos",
		},
	},
	team.FleetSec.TeamName: {
		readWriteDatabases: []string{
			"fleetsec_dev",
		},
	},
	team.Navigation.TeamName: {
		readWriteBuckets: []string{
			"commercial-navigation-daily-driver-session-count",
		},
	},
}

// findIntersectionOfBiztechenterprisedataAndBiztechProdReadWriteDbs finds the intersection of
// the readwriteDatabases for the biztechenterprisedata-cluster and biztech-prod-cluster IAM roles.
// This function returns 3 slices of strings:
// 1. The unique readwriteDatabase elements in biztech-prod (create as an inline policy)
// 2. The unique readwriteDatabase elements in biztechenterprisedata (create as an inline policy)
// 3. The intersecting readwriteDatabase elements between the 2 roles (extract into a managed policy
// that can be shared among the roles).
// Only handle these 2 roles for now because they are hitting the inline policy character limit, but
// we can extend this function later to migrate more inline policies for other roles to managed policies.
func findIntersectionOfBiztechenterprisedataAndBiztechProdReadWriteDbs(config dataplatformconfig.DatabricksConfig) (
	prodReadWriteDbDiff []string, enterpriseReadWriteDbDiff []string, readWriteIntersect []string, err error) {
	var biztechProdReadWrite, enterpriseDataReadWrite []string
	biztechProdName := "biztech-prod"
	biztechEnterpriseDataName := strings.ToLower(team.BizTechEnterpriseData.TeamName)
	for _, profile := range allRawClusterProfiles(config) {
		if profile.clusterName == "biztech-prod" {
			biztechProdReadWrite = profile.readwriteDatabases
		} else if profile.clusterName == biztechEnterpriseDataName {
			enterpriseDataReadWrite = profile.readwriteDatabases
		}
	}
	if len(biztechProdReadWrite) == 0 || len(enterpriseDataReadWrite) == 0 {
		return nil, nil, nil, oops.Errorf("could not find cluster profile for either %s or %s", biztechProdName, biztechEnterpriseDataName)
	}

	// Find the intersection between readwriteDatabses, which is used to populate the glue-database-write inline policy.
	prodReadWriteDbDiff, enterpriseReadWriteDbDiff, readWriteIntersect = findAndRemoveListIntersection(
		biztechProdReadWrite, enterpriseDataReadWrite)
	return prodReadWriteDbDiff, enterpriseReadWriteDbDiff, readWriteIntersect, nil
}

// findAndRemoveListIntersection takes in 2 slices of strings, and returns 3 slices:
// 1. the unique elements in list1 that do not intersect with list 2
// 2. the unique elements in list2 that do not intersect with list 1
// 3. the intersecting elements between list1 and list2
func findAndRemoveListIntersection(list1, list2 []string) (diffList1, diffList2, intersectList []string) {
	// Return early if either of the lists has no elements.
	if len(list1) == 0 || len(list2) == 0 {
		return list1, list2, []string{}
	}

	// Make a map of all list 1 elements.
	list1Map := make(map[string]bool)
	for _, item1 := range list1 {
		list1Map[item1] = true
	}

	// Identify the intersection between the 2 lists, and populate separate lists of diffed elements.
	for _, item2 := range list2 {
		// If the item from list 2 does not exist in list 1, add it to diffList2.
		if !list1Map[item2] {
			diffList2 = append(diffList2, item2)
		} else {
			// If the item exists in both lists, add it to the intersectList.
			// Delete intersecting items from list1Map so that we're only left with the items that differ from list 2.
			intersectList = append(intersectList, item2)
			delete(list1Map, item2)
		}
	}

	// list1Map now only contains elements that are different from list2. Convert the map to a list.
	for item := range list1Map {
		diffList1 = append(diffList1, item)
	}
	// Sort lists to ensure snapshot tests are predictable.
	sort.Strings(diffList1)
	sort.Strings(diffList2)
	sort.Strings(intersectList)
	return diffList1, diffList2, intersectList
}

func listToMap(l []string) map[string]struct{} {
	m := make(map[string]struct{})
	for _, item := range l {
		m[item] = struct{}{}
	}
	return m
}

func uniqueElements(input []string) []string {
	// Convert input to map to get unique set of keys.
	uniqueMap := make(map[string]struct{})
	for _, item := range input {
		uniqueMap[item] = struct{}{}
	}

	// Convert map back to a list.
	var uniqueList []string
	for key := range uniqueMap {
		uniqueList = append(uniqueList, key)
	}
	return uniqueList
}
