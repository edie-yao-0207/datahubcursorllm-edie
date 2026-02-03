from dagster import AssetExecutionContext, WeeklyPartitionsDefinition
from dataweb import table
from dataweb.userpkgs.constants import DATAENGINEERING, AWSRegion, Database, WarehouseWriteMode, TableType
from dataweb.userpkgs.utils import build_table_description, partition_key_ranges_from_context
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Day for which usage is being tracked"},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "org_name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Name of the organization"},
    },
    {
        "name": "org_category",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Category of org, based on licenses and internal users in corresponding account"},
    },
    {
        "name": "account_arr_segment",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "account_size_segment_name",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "account_billing_country",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Cloud region of the organization"},
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "account_id",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Unique ID of the account"},
    },
    {
        "name": "account_name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Name of the account"},
    },
    {
        "name": "enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Whether the org has access to the feature or not"},
    },
    {
        "name": "usage_weekly",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Feature usage in the last 7 days"},
    },
    {
        "name": "usage_monthly",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Feature usage in the last 28 days"},
    },
    {
        "name": "usage_prior_month",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Feature usage in the 28 days prior to the last 28 days"},
    },
    {
        "name": "usage_weekly_prior_month",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Feature usage in the 7 days prior to the last 28 days"},
    },
    {
        "name": "daily_active_user",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Count of active users for the day for the feature"},
    },
    {
        "name": "daily_enabled_users",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Count of distinct users who had access to use the feature on the day"},
    },
    {
        "name": "weekly_active_users",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Count of distinct users over last 7 days for the feature"},
    },
    {
        "name": "weekly_enabled_users",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Count of distinct users who had access to use the feature in the last 7 days"},
    },
    {
        "name": "monthly_active_users",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Count of distinct users over last 28 days for the feature"},
    },
    {
        "name": "monthly_enabled_users",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Count of distinct users who had access to use the feature in the last 28 days"},
    },
    {
        "name": "org_active_day",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Whether the org was active for the feature on the day or not"},
    },
    {
        "name": "org_active_week",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Whether the org was active for the feature in the last week or not"},
    },
    {
        "name": "org_active_week_prior_month",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Whether the org was active for the feature in the week 28 days ago or not"},
    },
    {
        "name": "org_active_month",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Whether the org was active for the feature in the last 28 days or not"},
    },
    {
        "name": "org_active_prior_month",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Whether the org was active for the feature in 28 days prior to the last 28 days or not"},
    },
    {
        "name": "feature",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Name of feature being tracked (transformed to lowercase with underscores)"},
    },
    {
        "name": "updated_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {"comment": "Timestamp when the feature adoption data was last updated in the upstream table."},
    },
    {
        "name": "is_beta",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Whether the feature is a beta feature (1) or not (0)"},
    },
]

QUERY = """--sql
    WITH customer_orgs AS (
        SELECT DISTINCT org_id
        FROM datamodel_core.dim_organizations
        WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_organizations WHERE date <= '{PARTITION_LATEST_DATE}')
            AND is_paid_customer = TRUE
    )
    SELECT
        *
        , CASE
            WHEN feature IN (
                'Roadside Parking',
                'Asset Utilization v2',
                'Custom Dashboards',
                'Functions',
                'Fuel Theft',
                'Functions',
                'Detention 2.0',
                'DVIR 2.0',
                'Pedestrian Collision Warning (Brigid)',
                'Pedestrian Collision Warning (Multicam)',
                'Route Planning',
                'Roadside Parking',
                'Worker Safety Mobile',
                'Worker Safety Wearable'
            ) THEN 1
            ELSE 0
        END AS is_beta
    FROM {PRODUCT_USAGE_GLOBAL_TABLE}
    INNER JOIN customer_orgs USING(org_id)  -- Filter out non-paying/non-customer orgs
    -- Note: agg_product_usage_global is queried with a static reference to PARTITION_LATEST_DATE,
    -- unlike other tables which use a dynamic reference to the latest available date.
    -- This is intentional since we don't want silent failures where the latest
    -- product usage data is not available.
    WHERE date = '{PARTITION_LATEST_DATE}'
--endsql
"""


@table(
    database=Database.FEATURE_STORE,
    description=build_table_description(
        table_desc="""A table containing a set of features used for generating product recommendations for each organization.""",
        row_meaning="""Each row represent an organization and its feature adoption for a given week""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by="Weekly on Monday by 12pm PST",
    ),
    schema=SCHEMA,
    primary_keys=["org_id", "date"],
    partitioning=WeeklyPartitionsDefinition(start_date="2025-10-01"),
    upstreams=[
        "datamodel_core.dim_organizations",
        "product_analytics.agg_product_usage_global",
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    regions=[AWSRegion.US_WEST_2, AWSRegion.EU_WEST_1],
    owners=[DATAENGINEERING],
)
def product_recommendations_adoption_features(context: AssetExecutionContext) -> DataFrame:
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
    if region != AWSRegion.US_WEST_2:
        # Product usage global table aggregates data from all regions and runs in the US only.
        # That table is then delta shared to other regions.
        PRODUCT_USAGE_GLOBAL_TABLE = "data_tools_delta_share.product_analytics.agg_product_usage_global"
    else:
        PRODUCT_USAGE_GLOBAL_TABLE = "default.product_analytics.agg_product_usage_global"

    query = QUERY.format(
        PARTITION_LATEST_DATE=PARTITION_LATEST_DATE,
        PRODUCT_USAGE_GLOBAL_TABLE=PRODUCT_USAGE_GLOBAL_TABLE,
    )
    context.log.info(f"{query}")

    df = (
        spark.sql(query)
        .where(F.col("is_beta") == 0)
        .withColumn("feature", F.regexp_replace(F.lower(F.col("feature")), r"[ -]+", "_"))
        .withColumn("date", F.lit(PARTITION_START))
    )
    context.log.info(f"df schema: {df.printSchema()}")

    return df
