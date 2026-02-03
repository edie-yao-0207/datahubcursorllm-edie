from dagster import AssetExecutionContext
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TrendDQCheck,
    table,
)
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
{USAGE_SQL}
-- Definitions:
-- usage_weekly: Usage in the last 7 days (enabled for CSL)
-- usage_monthly: Usage in the last 28 days (enabled for CSL)
-- usage_prior_month: Usage in the 28 days prior to the last 28 days (enabled for CSL)
-- usage_weekly_prior_month: Usage in the 7 days prior to the last 28 days (enabled for CSL)
-- daily_active_user: Number of unique users using the feature on the day in question (not used here)
-- daily_enabled_users: Number of unique users who had access to the feature on the day in question (not used here)
-- weekly_active_users: Number of unique users using the feature in the last 7 days (not used here)
-- weekly_enabled_users: Number of unique users who had access to the feature in the last 7 days (not used here)
-- monthly_active_users: Number of unique users using the feature in the last 28 days (not used here)
-- monthly_enabled_users: Number of unique users who had access to the feature in the last 28 days (not used here)
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
    CAST(0 AS BIGINT) AS daily_active_user,
    CAST(0 AS BIGINT) AS daily_enabled_users,
    CAST(0 AS BIGINT) AS weekly_active_users,
    CAST(0 AS BIGINT) AS weekly_enabled_users,
    CAST(0 AS BIGINT) AS monthly_active_users,
    CAST(0 AS BIGINT) AS monthly_enabled_users,
    COALESCE(e.org_active_day, 0) AS org_active_day,
    CASE WHEN usage_weekly > 0 THEN 1 ELSE 0 END AS org_active_week,
    CASE WHEN usage_weekly_prior_month > 0 THEN 1 ELSE 0 END AS org_active_week_prior_month,
    CASE WHEN usage_monthly > 0 THEN 1 ELSE 0 END AS org_active_month,
    CASE WHEN usage_prior_month > 0 THEN 1 ELSE 0 END AS org_active_prior_month
FROM final_relevant_orgs o
LEFT OUTER JOIN final_usage_metrics e
    ON e.org_id = o.org_id
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
    e.org_active_day
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""(Global) As part of Executive Scorecard, this shows usage stats for the Commercial Speed Limits feature""",
        row_meaning="""Each row represents an org's usage of Commercial Speed Limits over various time periods""",
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
        NonEmptyDQCheck(name="dq_non_empty_agg_executive_scorecard_csl_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_executive_scorecard_csl_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_executive_scorecard_csl_global",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        TrendDQCheck(
            name="dq_trend_agg_executive_scorecard_csl_global_enablement",
            lookback_days=1,
            tolerance=0.05,
            block_before_write=True,
        ),
    ],
    backfill_batch_size=1,
    upstreams=[
        "us-west-2:product_analytics_staging.stg_customer_enriched",
        "us-west-2|eu-west-1:product_analytics.dim_organizations_safety_settings",
        "eu-west-1:datamodel_core.dim_organizations",
        "eu-west-1:product_analytics_staging.dim_organizations_classification",
        "us-west-2|eu-west-1:finopsdb.customer_info",
        "us-west-2:datamodel_launchdarkly_bronze.segment_flag",
        "us-west-2:datamodel_launchdarkly_bronze.rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_variation",
        "us-west-2:release_management.ld_flag_variation_index_map",
    ],
)
def agg_executive_scorecard_csl_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_executive_scorecard_csl_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    json_config = load_product_usage_config("csl")
    relevant_orgs = get_final_relevant_orgs(json_config, PARTITION_START)
    usage_sql = get_final_usage_sql(json_config, PARTITION_START)
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        RELEVANT_ORGS=relevant_orgs,
        USAGE_SQL=usage_sql,
    )
    context.log.info(f"{query}")
    return query
