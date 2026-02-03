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
    get_enabled_users_sql,
    get_final_relevant_orgs,
    get_final_usage_sql,
    load_product_usage_config,
)


QUERY = """
WITH {RELEVANT_ORGS}
{ENABLED_USERS_SQL}
{USAGE_SQL}
-- Definitions:
-- usage_weekly: Usage in the last 7 days (opening the build from AI feature)
-- usage_monthly: Usage in the last 28 days (opening the build from AI feature)
-- usage_prior_month: Usage in the 28 days prior to the last 28 days (opening the build from AI feature)
-- usage_weekly_prior_month: Usage in the 7 days prior to the last 28 days (opening the build from AI feature)
-- daily_active_user: Number of unique users using the feature on the day in question (in this context, opening the build from AI feature)
-- daily_enabled_users: Number of unique users who had access to the feature on the day in question
-- weekly_active_users: Number of unique users using the feature in the last 7 days (in this context, opening the build from AI feature)
-- weekly_enabled_users: Number of unique users who had access to the feature in the last 7 days
-- monthly_active_users: Number of unique users using the feature in the last 28 days (in this context, opening the build from AI feature)
-- monthly_enabled_users: Number of unique users who had access to the feature in the last 28 days
-- org_active_day: Whether the org used the feature on the day in question
-- org_active_week: Whether the org used the feature in the last 7 days
-- org_active_week_prior_month: Whether the org used the feature in the 7 days prior to the last 28 days
-- org_active_month: Whether the org used the feature in the last 28 days
-- org_active_prior_month: Whether the org used the feature in the 28 days prior to the last 28 days
SELECT
    '{PARTITION_START}' AS date,
    ga.org_id,
    ga.org_name,
    ga.org_category,
    ga.account_arr_segment,
    ga.account_size_segment_name,
    ga.account_billing_country,
    ga.region,
    ga.sam_number,
    ga.account_id,
    ga.account_name,
    1 AS enabled,
    COALESCE(t.usage_weekly, 0) AS usage_weekly,
    COALESCE(t.usage_monthly, 0) AS usage_monthly,
    COALESCE(t.usage_prior_month, 0) AS usage_prior_month,
    COALESCE(t.usage_weekly_prior_month, 0) AS usage_weekly_prior_month,
    COALESCE(t.daily_active_user, 0) AS daily_active_user,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= '{PARTITION_START}' THEN eu.user_id END), 0) AS daily_enabled_users,
    COALESCE(t.weekly_active_users, 0) AS weekly_active_users,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= DATE_SUB('{PARTITION_START}', 6) THEN eu.user_id END), 0) AS weekly_enabled_users,
    COALESCE(t.monthly_active_users, 0) AS monthly_active_users,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= DATE_SUB('{PARTITION_START}', 27) THEN eu.user_id END), 0) AS monthly_enabled_users,
    COALESCE(t.org_active_day, 0) AS org_active_day,
    CASE WHEN usage_weekly > 0 THEN 1 ELSE 0 END AS org_active_week,
    CASE WHEN usage_weekly_prior_month > 0 THEN 1 ELSE 0 END AS org_active_week_prior_month,
    CASE WHEN usage_monthly > 0 THEN 1 ELSE 0 END AS org_active_month,
    CASE WHEN usage_prior_month > 0 THEN 1 ELSE 0 END AS org_active_prior_month
FROM final_relevant_orgs ga
LEFT OUTER JOIN final_usage_metrics t
    ON t.org_id = ga.org_id
LEFT OUTER JOIN enabled_users eu
    ON ga.org_id = eu.org_id
GROUP BY
    ga.org_id,
    ga.org_name,
    ga.org_category,
    ga.account_arr_segment,
    ga.account_size_segment_name,
    ga.account_billing_country,
    ga.region,
    ga.sam_number,
    ga.account_id,
    ga.account_name,
    t.usage_weekly,
    t.usage_monthly,
    t.usage_prior_month,
    t.usage_weekly_prior_month,
    t.daily_active_user,
    t.weekly_active_users,
    t.monthly_active_users,
    t.org_active_day
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""(Global) As part of Executive Scorecard, this shows usage stats for the AI Report Builder Entrypoint feature""",
        row_meaning="""Each row represents an org's usage of AI Report Builder Entrypoint over various time periods""",
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
        NonEmptyDQCheck(name="dq_non_empty_agg_executive_scorecard_ai_report_builder_entrypoint_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_executive_scorecard_ai_report_builder_entrypoint_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_executive_scorecard_ai_report_builder_entrypoint_global",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_ai_report_builder_entrypoint_global_enabled_users",
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
            name="dq_sql_agg_executive_scorecard_ai_report_builder_entrypoint_global_usage",
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
            name="dq_sql_agg_executive_scorecard_ai_report_builder_entrypoint_global_high_usage",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE usage_weekly > 10000
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_ai_report_builder_entrypoint_global_high_active_users",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE weekly_active_users > 10000
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        TrendDQCheck(name="dq_trend_agg_executive_scorecard_ai_report_builder_entrypoint_global_enablement", lookback_days=1, tolerance=0.02),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_ai_report_builder_entrypoint_global_usage_change",
            sql_query="""
                WITH today_usage AS (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS today_count
                    FROM df
                ),
                last_week_usage as (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS last_week_count
                    FROM product_analytics_staging.agg_executive_scorecard_ai_report_builder_entrypoint_global
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
        "us-west-2:mixpanel_samsara.ai_chat_advanced_custom_report_chat_entrypoint",
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1:product_analytics_staging.dim_organizations_classification",
        "us-west-2|eu-west-1:datamodel_platform_bronze.raw_clouddb_users_organizations",
        "us-west-2|eu-west-1:datamodel_platform_bronze.raw_clouddb_users",
        "us-west-2|eu-west-1:datamodel_platform.dim_users_organizations_permissions",
        "us-west-2|eu-west-1:finopsdb.customer_info",
    ],
)
def agg_executive_scorecard_ai_report_builder_entrypoint_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_executive_scorecard_ai_report_builder_entrypoint_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    json_config = load_product_usage_config("ai_report_builder_entrypoint")
    relevant_orgs = get_final_relevant_orgs(json_config, PARTITION_START)
    usage_sql = get_final_usage_sql(json_config, PARTITION_START)
    enabled_users_sql = get_enabled_users_sql(json_config, PARTITION_START)
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        RELEVANT_ORGS=relevant_orgs,
        USAGE_SQL=usage_sql,
        ENABLED_USERS_SQL=enabled_users_sql,
    )
    context.log.info(f"{query}")
    return query
