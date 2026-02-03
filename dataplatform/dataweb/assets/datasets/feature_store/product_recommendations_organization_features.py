from dagster import AssetExecutionContext, WeeklyPartitionsDefinition
from dataweb import NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import DATAENGINEERING, AWSRegion, Database, WarehouseWriteMode, TableType
from dataweb.userpkgs.utils import build_table_description, partition_key_ranges_from_context
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

PRIMARY_KEYS = ["org_id", "date"]

# Fields that should never be NULL based on query logic:
# - Primary keys (org_id, date)
# - Fields coalesced to a default, e.g.:
#     - tenure (coalesced to 0)
#     - all count/statistics fields (coalesced to 0)
#     - account_arr_segment (coalesced to 'unknown')
# - All organization-level categorical fields derived from stg_organization_categories or similar upstreams (guaranteed non-null by upstream queries)
# If editing schema or queries, ensure any new fields expected to be non-null are included here.
NON_NULL_COLUMNS = [
    "org_id",
    "date",
    "tenure",
    "account_arr_segment",
    "avg_mileage",
    "region",
    "fleet_size",
    "industry_vertical",
    "fuel_category",
    "primary_driving_environment",
    "fleet_composition",
    "vg_n",
    "cm_n",
    "at_n",
    "ag_n",
    "sg_n",
    "app_users_n",
    "web_users_n",
    "drivers_n",
    "drivers_eld_exempt_n",
    "acr_assets_n",
    "acr_drivers_n",
    "acr_trips_n",
    "acr_safetyevents_n",
    "acr_dvir_n",
    "acr_speeding_n",
    "acr_hos_n",
    "acr_idle_n",
]

SCHEMA = [
    {"name": "org_id", "type": "long", "nullable": False, "metadata": {}},
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "The date the features were generated."},
    },
    {"name": "tenure", "type": "long", "nullable": False, "metadata": {}},
    {"name": "account_arr_segment", "type": "string", "nullable": False, "metadata": {}},
    {"name": "avg_mileage", "type": "string", "nullable": False, "metadata": {}},
    {"name": "region", "type": "string", "nullable": False, "metadata": {}},
    {"name": "fleet_size", "type": "string", "nullable": False, "metadata": {}},
    {"name": "industry_vertical", "type": "string", "nullable": False, "metadata": {}},
    {"name": "fuel_category", "type": "string", "nullable": False, "metadata": {}},
    {
        "name": "primary_driving_environment",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {"name": "fleet_composition", "type": "string", "nullable": False, "metadata": {}},
    {"name": "vg_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "cm_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "at_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "ag_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "sg_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "app_users_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "web_users_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "drivers_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "drivers_eld_exempt_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "acr_assets_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "acr_drivers_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "acr_trips_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "acr_safetyevents_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "acr_dvir_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "acr_speeding_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "acr_hos_n", "type": "long", "nullable": False, "metadata": {}},
    {"name": "acr_idle_n", "type": "long", "nullable": False, "metadata": {}},
]

QUERY = """--sql
    WITH customer_orgs AS (
        SELECT 
            org_id
            -- account_arr_segment is guaranteed non-null by coalesce() upstream
            , MIN(account_arr_segment) AS account_arr_segment
        FROM datamodel_core.dim_organizations
        WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_organizations WHERE date <= '{PARTITION_LATEST_DATE}')
            AND is_paid_customer = TRUE
        GROUP BY org_id
    ),
    device_counts AS (  -- Active devices within past month
        SELECT 
            org_id
            , COUNT(DISTINCT CASE WHEN device_type = 'VG - Vehicle Gateway' THEN device_id END) AS vg_n
            , COUNT(DISTINCT CASE WHEN device_type = 'CM - AI Dash Cam'     THEN device_id END) AS cm_n
            , COUNT(DISTINCT CASE WHEN device_type = 'AT - Asset Tracker'   THEN device_id END) AS at_n
            , COUNT(DISTINCT CASE WHEN device_type = 'AG - Asset Gateway'   THEN device_id END) AS ag_n
            , COUNT(DISTINCT CASE WHEN device_type = 'SG - Site Gateway'    THEN device_id END) AS sg_n
        FROM datamodel_core.lifetime_device_activity
        WHERE date = (SELECT MAX(date) FROM datamodel_core.lifetime_device_activity WHERE date <= '{PARTITION_LATEST_DATE}')
            AND l28 > 0
            AND device_type != 'unknown'
        GROUP BY org_id
        ORDER BY org_id
    ),
    user_counts AS ( -- Web and app users within past month
        SELECT 
            org_id
            , COUNT(DISTINCT CASE WHEN DATE_DIFF(date, last_fleet_app_usage_date) <= 28 THEN user_id END) AS app_users_n
            , COUNT(DISTINCT CASE WHEN DATE_DIFF(date, last_web_usage_date) <= 28 THEN user_id END) AS web_users_n
        FROM datamodel_platform.dim_users_organizations
        WHERE date = (SELECT MAX(date) FROM datamodel_platform.dim_users_organizations WHERE date <= '{PARTITION_LATEST_DATE}')
            AND is_samsara_email = FALSE
        GROUP BY org_id
    ),
    driver_counts AS ( -- Drivers within the past month
        SELECT 
            org_id
            , COUNT(DISTINCT CASE WHEN eld_exempt = 1 THEN driver_id END) AS drivers_eld_exempt_n
            , COUNT(DISTINCT CASE WHEN eld_exempt = 0 THEN driver_id END) AS drivers_n
        FROM datamodel_platform.dim_drivers
        WHERE date = (SELECT MAX(date) FROM datamodel_platform.dim_drivers WHERE date <= '{PARTITION_LATEST_DATE}')
            AND is_deleted = FALSE
            AND DATEDIFF(date, last_mobile_login_date) <= 28
        GROUP BY org_id
    ),
    acr_runs AS (
    SELECT 
        org_id
        , COUNT(DISTINCT CASE WHEN report_run_metadata.report_config.data_set = "Assets" THEN uuid END) AS acr_assets_n
        , COUNT(DISTINCT CASE WHEN report_run_metadata.report_config.data_set = "Drivers" THEN uuid END) AS acr_drivers_n
        , COUNT(DISTINCT CASE WHEN report_run_metadata.report_config.data_set = "Trips" THEN uuid END) AS acr_trips_n
        , COUNT(DISTINCT CASE WHEN report_run_metadata.report_config.data_set = "SafetyEvents" THEN uuid END) AS acr_safetyevents_n
        , COUNT(DISTINCT CASE WHEN report_run_metadata.report_config.data_set = "Dvirs" THEN uuid END) AS acr_dvir_n
        , COUNT(DISTINCT CASE WHEN report_run_metadata.report_config.data_set = "SpeedingIntervals" THEN uuid END) AS acr_speeding_n
        , COUNT(DISTINCT CASE WHEN report_run_metadata.report_config.data_set = "HosLogs" THEN uuid END) AS acr_hos_n
        , COUNT(DISTINCT CASE WHEN report_run_metadata.report_config.data_set = "IdleEvents" THEN uuid END) AS acr_idle_n
    FROM reportconfigdb_shards.report_runs
    WHERE DATEDIFF(CAST('{PARTITION_LATEST_DATE}' AS DATE), date) <= 28 -- Reports run in the 28 days prior to the partition date
        AND status = 2        -- completed runs
        AND is_ephemeral = 0  -- remove unsaved/preview runs
        AND report_run_metadata.report_config.data_set IS NOT NULL
    GROUP BY org_id
    )

    SELECT 
        '{PARTITION_START}' AS date
        , org_id
        -- tenure_months appears to be one of the few features in stg_organization_categories
        -- that isn't coalesced to 0.
        , COALESCE(tenure_months, 0) AS tenure
        , customer_orgs.account_arr_segment
        , avg_mileage
        , region
        , fleet_size
        , industry_vertical
        , fuel_category
        , primary_driving_environment
        , fleet_composition
        , COALESCE(device_counts.vg_n, 0) AS vg_n
        , COALESCE(device_counts.cm_n, 0) AS cm_n
        , COALESCE(device_counts.at_n, 0) AS at_n
        , COALESCE(device_counts.ag_n, 0) AS ag_n
        , COALESCE(device_counts.sg_n, 0) AS sg_n
        , COALESCE(user_counts.app_users_n, 0) AS app_users_n
        , COALESCE(user_counts.web_users_n, 0) AS web_users_n
        , COALESCE(driver_counts.drivers_n, 0) AS drivers_n
        , COALESCE(driver_counts.drivers_eld_exempt_n, 0) AS drivers_eld_exempt_n
        , COALESCE(acr_runs.acr_assets_n, 0) AS acr_assets_n
        , COALESCE(acr_runs.acr_drivers_n, 0) AS acr_drivers_n
        , COALESCE(acr_runs.acr_trips_n, 0) AS acr_trips_n
        , COALESCE(acr_runs.acr_safetyevents_n, 0) AS acr_safetyevents_n
        , COALESCE(acr_runs.acr_dvir_n, 0) AS acr_dvir_n
        , COALESCE(acr_runs.acr_speeding_n, 0) AS acr_speeding_n
        , COALESCE(acr_runs.acr_hos_n, 0) AS acr_hos_n
        , COALESCE(acr_runs.acr_idle_n, 0) AS acr_idle_n
    FROM {ORG_CATEGORIES_TABLE}
    INNER JOIN customer_orgs USING(org_id)        -- Filter to current paid customers
    LEFT OUTER JOIN device_counts USING(org_id)
    LEFT OUTER JOIN user_counts USING(org_id)
    LEFT OUTER JOIN driver_counts USING(org_id)
    LEFT OUTER JOIN acr_runs USING (org_id)
    WHERE date = (SELECT MAX(date) FROM {ORG_CATEGORIES_TABLE} WHERE date <= '{PARTITION_LATEST_DATE}')
--endsql
"""


@table(
    database=Database.FEATURE_STORE,
    description=build_table_description(
        table_desc="""Organization-level features table used for product recommendations, reflecting adoption, tenure, device/user/driver metrics, and other org characteristics sourced from joined platform datasets.""",
        row_meaning="""Each row represents an organization with its aggregated features for a given week""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by="Weekly on Monday by 12pm PST",
    ),
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=WeeklyPartitionsDefinition(start_date="2025-10-01"),
    dq_checks=[
        NonNullDQCheck(
            name="dq_non_null_product_recommendations_organization_features",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_product_recommendations_organization_features",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "product_analytics_staging.stg_organization_categories",
        "reportconfigdb_shards.report_runs",
        "datamodel_platform.dim_drivers",
        "datamodel_platform.dim_users_organizations",
        "datamodel_core.dim_organizations",
        "datamodel_core.lifetime_device_activity",
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    regions=[AWSRegion.US_WEST_2, AWSRegion.EU_WEST_1],
    owners=[DATAENGINEERING],
)
def product_recommendations_organization_features(context: AssetExecutionContext) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    context.log.info(f"Partition start: {PARTITION_START}; type: {type(PARTITION_START)}")
    # Since we did not specify an end_offset in the partitions definition above,
    # the last partition in the set will end before the current time.
    # Additionally, WeeklyPartitionsDefinition defaults to starting the week on Sunday.
    # Therefore, adding 6 days to the start date of the partition will give us 
    # the date of the last full week in the partition set.
    PARTITION_LATEST_DATE = str((datetime.strptime(PARTITION_START, "%Y-%m-%d") + timedelta(days=6)).strftime("%Y-%m-%d"))
    context.log.info(f"Partition latest date: {PARTITION_LATEST_DATE}; type: {type(PARTITION_LATEST_DATE)}")

    region = context.asset_key.path[0]
    if region == AWSRegion.EU_WEST_1:
        ORG_CATEGORIES_TABLE = "product_analytics_staging.stg_organization_categories_eu"
    else:
        ORG_CATEGORIES_TABLE = "product_analytics_staging.stg_organization_categories"

    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_LATEST_DATE=PARTITION_LATEST_DATE,
        ORG_CATEGORIES_TABLE=ORG_CATEGORIES_TABLE,
    )
    context.log.info(f"{query}")

    df = spark.sql(query)
    context.log.info(f"df schema: {df.printSchema()}")

    return df
