from dagster import AssetExecutionContext
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    TrendDQCheck,
    table,
)
from dataweb._core.dq_utils import Operator
from dataweb.userpkgs.executive_scorecard_constants import (
    MAX_RETRIES,
    PARTITIONING,
    PRIMARY_KEYS,
    SCHEMA,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)
from dataweb.userpkgs.product_usage_utils import (
    get_final_relevant_orgs,
    get_final_usage_sql,
    load_product_usage_config,
)


QUERY = """
WITH {RELEVANT_ORGS}
enabled_drivers_us AS (
    -- We calculate drivers separately as it's more efficient
    SELECT
      dd.org_id AS org_id,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= '{PARTITION_START}' THEN dd.driver_id END) AS daily_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 6) THEN dd.driver_id END) AS weekly_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27) THEN dd.driver_id END) AS monthly_enabled_drivers
    FROM datamodel_platform.dim_drivers dd
    JOIN final_relevant_orgs o
        ON dd.org_id = o.org_id
    WHERE
        dd.date = '{PARTITION_START}'
        AND (DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27))
    GROUP BY dd.org_id
),
enabled_drivers_eu AS (
    -- We calculate drivers separately as it's more efficient
    SELECT
      dd.org_id AS org_id,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= '{PARTITION_START}' THEN dd.driver_id END) AS daily_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 6) THEN dd.driver_id END) AS weekly_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27) THEN dd.driver_id END) AS monthly_enabled_drivers
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers` dd
    JOIN final_relevant_orgs o
        ON dd.org_id = o.org_id
    WHERE
        dd.date = '{PARTITION_START}'
        AND (DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27))
    GROUP BY dd.org_id
),
enabled_drivers AS (
    SELECT * FROM enabled_drivers_us

    UNION ALL

    SELECT * FROM enabled_drivers_eu
),
{USAGE_SQL}
-- Definitions:
-- usage_weekly: Usage in the last 7 days (DVIR submissions)
-- usage_monthly: Usage in the last 28 days (DVIR submissions)
-- usage_prior_month: Usage in the 28 days prior to the last 28 days (DVIR submissions)
-- usage_weekly_prior_month: Usage in the 7 days prior to the last 28 days (DVIR submissions)
-- daily_active_user: Number of unique users using the feature on the day in question (drivers submitting DVIRs)
-- daily_enabled_users: Number of unique users who had access to the feature on the day in question (drivers)
-- weekly_active_users: Number of unique users using the feature in the last 7 days (drivers submitting DVIRs)
-- weekly_enabled_users: Number of unique users who had access to the feature in the last 7 days (drivers)
-- monthly_active_users: Number of unique users using the feature in the last 28 days (drivers submitting DVIRs)
-- monthly_enabled_users: Number of unique users who had access to the feature in the last 28 days (drivers)
-- org_active_day: Whether the org used the feature on the day in question
-- org_active_week: Whether the org used the feature in the last 7 days
-- org_active_week_prior_month: Whether the org used the feature in the 7 days prior to the last 28 days
-- org_active_month: Whether the org used the feature in the last 28 days
-- org_active_prior_month: Whether the org used the feature in the 28 days prior to the last 28 days
SELECT
    '{PARTITION_START}' AS date,
    o.org_id,
    o.org_name,
    o.org_category,
    o.account_arr_segment,
    o.account_size_segment_name,
    o.account_billing_country,
    o.region,
    o.sam_number,
    o.account_id,
    o.account_name,
    1 AS enabled,
    COALESCE(e.usage_weekly, 0) AS usage_weekly,
    COALESCE(e.usage_monthly, 0) AS usage_monthly,
    COALESCE(e.usage_prior_month, 0) AS usage_prior_month,
    COALESCE(e.usage_weekly_prior_month, 0) AS usage_weekly_prior_month,
    COALESCE(e.daily_active_user, 0) AS daily_active_user,
    COALESCE(MAX(ed.daily_enabled_drivers), 0) AS daily_enabled_users,
    COALESCE(e.weekly_active_users, 0) AS weekly_active_users,
    COALESCE(MAX(ed.weekly_enabled_drivers), 0) AS weekly_enabled_users,
    COALESCE(e.monthly_active_users, 0) AS monthly_active_users,
    COALESCE(MAX(ed.monthly_enabled_drivers), 0) AS monthly_enabled_users,
    COALESCE(e.org_active_day, 0) AS org_active_day,
    CASE WHEN usage_weekly > 0 THEN 1 ELSE 0 END AS org_active_week,
    CASE WHEN usage_weekly_prior_month > 0 THEN 1 ELSE 0 END AS org_active_week_prior_month,
    CASE WHEN usage_monthly > 0 THEN 1 ELSE 0 END AS org_active_month,
    CASE WHEN usage_prior_month > 0 THEN 1 ELSE 0 END AS org_active_prior_month
FROM final_relevant_orgs o
LEFT OUTER JOIN final_usage_metrics e
    ON e.org_id = o.org_id
LEFT OUTER JOIN enabled_drivers ed
    ON o.org_id = ed.org_id
GROUP BY
    o.org_id,
    o.org_name,
    o.org_category,
    o.account_arr_segment,
    o.account_size_segment_name,
    o.account_billing_country,
    o.region,
    o.sam_number,
    o.account_id,
    o.account_name,
    e.usage_weekly,
    e.usage_monthly,
    e.usage_prior_month,
    e.usage_weekly_prior_month,
    e.daily_active_user,
    e.weekly_active_users,
    e.monthly_active_users,
    e.org_active_day
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""(Global) As part of Executive Scorecard, this shows usage stats for the DVIR 2.0 feature""",
        row_meaning="""Each row represents an org's usage of DVIR 2.0 over various time periods""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=PARTITIONING,
    write_mode=WarehouseWriteMode.OVERWRITE,
    max_retries=MAX_RETRIES,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_agg_executive_scorecard_dvir2_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_executive_scorecard_dvir2_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_executive_scorecard_dvir2_global",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_dvir2_global_enabled_users",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE weekly_enabled_users < weekly_active_users
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_dvir2_global_usage",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE usage_monthly < usage_weekly
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_dvir2_global_high_usage",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE usage_weekly > 1000000
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_dvir2_global_high_active_users",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE weekly_active_users > 50000
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        TrendDQCheck(name="dq_trend_agg_executive_scorecard_dvir2_global_enablement", lookback_days=1, tolerance=0.02),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_dvir2_global_usage_change",
            sql_query="""
                WITH today_usage AS (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS today_count
                    FROM df
                ),
                last_week_usage as (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS last_week_count
                    FROM product_analytics_staging.agg_executive_scorecard_dvir2_global
                    WHERE date = (SELECT DISTINCT DATE_SUB(date, 7) FROM df)
                )
                SELECT CASE WHEN last_week_count = 0 THEN 0
                            ELSE ABS(today_count - last_week_count) * 1.0 / last_week_count
                            END AS observed_value
                FROM today_usage
                CROSS JOIN last_week_usage
            """,
            expected_value=0.2,
            operator=Operator.lte,
            block_before_write=False,
        ),
    ],
    backfill_batch_size=1,
    upstreams=[
        "us-west-2:product_analytics_staging.stg_customer_enriched",
        "us-west-2|eu-west-1:formsdb_shards.form_submissions",
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1:product_analytics_staging.dim_organizations_classification",
        "us-west-2|eu-west-1:finopsdb.customer_info",
        "us-west-2|eu-west-1:datamodel_platform.dim_drivers",
        "us-west-2:datamodel_launchdarkly_bronze.segment_flag",
        "us-west-2:datamodel_launchdarkly_bronze.rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_variation",
        "us-west-2|eu-west-1:releasemanagementdb_shards.feature_packages",
        "us-west-2|eu-west-1:releasemanagementdb_shards.feature_package_self_serve",
        "us-west-2|eu-west-1:clouddb.organizations",
        "us-west-2:release_management.ld_flag_variation_index_map",
    ],
)
def agg_executive_scorecard_dvir2_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_executive_scorecard_dvir2_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    json_config = load_product_usage_config("dvir2")
    relevant_orgs = get_final_relevant_orgs(json_config, PARTITION_START)
    usage_sql = get_final_usage_sql(json_config, PARTITION_START)
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        RELEVANT_ORGS=relevant_orgs,
        USAGE_SQL=usage_sql,
    )
    context.log.info(f"{query}")
    return query
