rds_dbs = [
    "alertsdb",
    "api2db_shards",
    "associationsdb_shards",
    "attributedb_shards",
    "auditsdb_shards",
    "clouddb",
    "cmassetsdb_shards",
    "coachingdb_shards",
    "compliancedb_shards",
    "csvuploadsdb_shards",
    "deploydb",
    "developersdb_shards",
    "deviceassociationsdb_shards",
    "dispatchdb_shards",
    "driverassignmentsdb_shards",
    "driverdocumentsdb_shards",
    "eldhosdb_shards",
    "eldeventsdb_shards",
    "encryptionkeydb",
    "eurofleetdb_shards",
    "finopsdb",
    "firmwaredb",
    "formsdb_shards",
    "fuelcardsdb_shards",
    "fueldb_shards",
    "geoeventsdb_shards",
    "industrialcoredb_shards",
    "internaldb",
    "jobschedulesdb_shards",
    "licenseentitydb",
    "localedb",
    "mdmdb_shards",
    "mediacatalogdb_shards",
    "messagesdb_shards",
    "mobiledb_shards",
    "oauth2db_shards",
    "oemdb_shards",
    "pingschedulesdb_shards",
    "productsdb",
    "recognitiondb_shards",
    "releasemanagementdb_shards",
    "remotesupportdb_shards",
    "reportconfigdb_shards",
    "retentiondb_shards",
    "safetydb_shards",
    "safetyeventingestiondb_shards",
    "safetyeventreviewdb",
    "safetyeventtriagedb_shards",
    "signalpromotiondb",
    "speedlimitsdb",
    "statsdb",
    "tachographdb_shards",
    "timecardsdb_shards",
    "trainingdb_shards",
    "trips2db_shards",
    "tripsdb_shards",
    "userorgpreferencesdb_shards",
    "userpreferencesdb",
    "vindb_shards",
    "workflowsdb_shards",
    "workforcevideodb_shards",
    "appconfigs",
    "maintenancedb_shards",
    "fuelcardintegrationsdb",
    "emissionsreportingdb_shards",
    "sensorconfigdb_shards",
    "safetyreportingdb_shards",
    "eucompliancedb_shards",
    "sessionsdb",
    "engineactivitydb_shards",
]

datastreams_dbs = [
    "mobile_logs",
    "api_logs",
    "datastreams",
    "datastreams_errors",
    "datastreams_schema",
]

datamodel_dbs = [
    "datamodel_core",
    "datamodel_telematics",
    "datamodel_safety",
    "datamodel_platform",
    "product_analytics",
    "feature_store",
    "inference_store",
]

datamodel_silver_dbs = [
    "product_analytics_staging",
]

biztech_dbs = [
    "biztech_edw_accouting_dbx_gold",
    "biztech_edw_csops_gold",
    "biztech_edw_customer_success_gold",
    "biztech_edw_dataquality_gold",
    "biztech_edw_extracts_gold",
    "biztech_edw_finance_strategy_gold",
    "biztech_edw_netsuite2_sb1",
    "biztech_edw_netsuite_sb1",
    "biztech_edw_netsuite_sb2",
    "biztech_edw_salescomp_gold",
    # "biztech_edw_salesforce_uat",  # very large db
    "biztech_edw_seops_gold",
    "biztech_edw_silver",
    "biztech_edw_supply_chain_gold",
    "biztech_edw_ti_silver",
    "biztech_edw_xactly_silver",
    "biztech_edws_adaptive_bronze",
    "biztech_edws_greenhouse_bronze",
    "biztech_edws_peopleops_gold",
    "biztech_edws_peopleops_silver",
    "biztech_edws_workday_bronze",
    "biztech_edws_xactly_bronze",
    "biztech_edws_zendesk_bronze",
]

datapipelines_dbs = [
    "perf_infra",
    "activity_report",
    "customer360",
    "trip_history_report",
    "start_stop_report",
    "speeding_incident_report",
    "samsara_zendesk",
    "safety_report",
    "objectstat_diffs",
    "material_usage_report",
    "ifta_report",
    "idle_locations_report",
    "fuel_energy_efficiency_report",
    "ev_charging_report",
    "engine_state",
    "engine_state_report",
    "ecodriving_report",
    "driver_app_engagement_report",
    "dataplatform_stable_report",
    "dataplatform_beta_report",
    "cm_health_report",
    "camera_connector_health",
    "api_logs_report",
    "dispatch_routes_report",
    "kinesisstats_window",
    "apptelematics",
    "canonical_distance",
    "time_on_site_report",
    "pto_input",
    "start_stop_report",
]

non_domain_dbs = [
    "billing",
    "compliance",
    "dataprep",
    "dataplatform",
    "growth",
    "data_analytics",
    "firmware",
    "firmware_dev",
    "data_analytics",
    "dataengineering",
    "dataprep_assets",
    "dataprep_cellular",
    "dataprep_firmware",
    "dataprep_ml",
    "dataprep_routing",
    "dataprep_safety",
    "dataprep_telematics",
    "adasmetrics",
    "apollo",
    "baxter",
    "bigquery",
    "cm3xinvestigation",
    "compliance_metrics",
    "connectedworker",
    "databricks_alerts",
    "dataproducts",
    "devexp",
    "firebase_crashlytics",
    "firmwarerelease",
    "gtms",
    "ksdifftool",
    "mdm",
    "mldatasets",
    "mobile_metrics",
    "multicam",
    "vgdiagnostics",
    # final dbs to add
    "datascience",
    "devecosystem",
    "dispatchdb",
    "dojo",
    "finops",
    "fivetran_netsuite_finance",
    "fuel_maintenance",
    "gql_query_performance_report",
    "hardware",
    "hardware_analytics",
    "helpers",
    "labelbox",
    "marketingdata",
    "mda_sandbox",
    "mda_stage",
    "netsuite_data",
    "netsuite_suiteanalytics",
    "opsmetrics",
    "pagerduty_bronze",
    "people_analytics",
    "platops",
    "release_management",
    "report_staging",
    "routeload_logs",
    "s3inventory",
    "sda_gold",
    "sfdc_data",
    "shared_people_ops_data",
    "solutions_integration_services",
    "supportops",
    "technical_support",
    "trips_transformations",
    "year_review_metrics",
    "auditlog",
    "fivetran_launchdarkly_bronze",
    "accuweather",
    "mediaexperience_prod",
    "canonical_distance_updates",
    "alert_latency",
    "aria_report",
    "hardware_features",
    "incidentio_bronze",
]

all_dbs = [
    *datamodel_dbs,
    *datamodel_silver_dbs,
    "kinesisstats",
    *rds_dbs,
    *non_domain_dbs,
    *datastreams_dbs,
    *datapipelines_dbs,
    *biztech_dbs,
    "dynamodb",
    "metrics_repo",
    "definitions",
    "s3bigstats",
]

edw_csops_gold_dbs = [
    "csops_gold",
]

edw_extract_gold_dbs = [
    "extracts_gold",
]

edw_cs_gold_dbs = [
    "customer_success_gold",
]

edw_fns_gold_dbs = [
    "finance_strategy_gold",
]

edw_act_gold_dbs = [
    "accounting_dbx_gold",
]

edw_seops_gold_dbs = [
    "seops_gold",
]

edw_salescomp_gold_dbs = [
    "salescomp_gold",
]

edw_sales_gold_dbs = [
    "sales_gold",
]

# Constants for dbt Cloud API
DBT_CLOUD_ACCOUNT_ID = 120474

# Team-specific job IDs for dbt Cloud
DBT_CLOUD_JOB_IDS = {
    "biztech": {
        "model": 658241,
        "docs": 837234,
    },
    "gtm": {
        "model": 850905,
        "docs": 850905,
    },
    # Add more teams as needed
}

DBT_CLOUD_BASE_URL = "https://cloud.getdbt.com/api/v2/accounts"
