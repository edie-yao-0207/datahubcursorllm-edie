from dagster import AssetExecutionContext
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TrendDQCheck,
    table,
)
from dataweb.userpkgs.executive_scorecard_constants import (
    INITIAL_LAUNCHDARKLY_DATE,
    MAX_RETRIES,
    PARTITIONING,
    PRIMARY_KEYS,
    SCHEMA,
)
from dataweb.userpkgs.constants import (
    ACCOUNT_BILLING_COUNTRY,
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
relevant_segments AS (
    -- Get any segments for the FF
    SELECT *
    FROM datamodel_launchdarkly_bronze.segment_flag
    WHERE
        key = '{flag_name}'
        AND date = (
            SELECT MAX(date) FROM datamodel_launchdarkly_bronze.segment_flag
            WHERE date <= GREATEST('{PARTITION_START}', '{INITIAL_LAUNCHDARKLY_DATE}')
        )
),
variations AS (
    -- Get all variations for the FF
    SELECT
        ffv.value,
        vim.variation_index
    FROM datamodel_launchdarkly_bronze.feature_flag_variation ffv
    JOIN release_management.ld_flag_variation_index_map vim
        ON ffv.feature_flag_key = vim.key
        AND UPPER(ffv.value) = UPPER(vim.variation_value)
    WHERE
        ffv.feature_flag_key = '{flag_name}'
        AND ffv.date = (
            SELECT MAX(date) FROM datamodel_launchdarkly_bronze.feature_flag_variation
            WHERE date <= GREATEST('{PARTITION_START}', '{INITIAL_LAUNCHDARKLY_DATE}')
        )
),
segment_parsing AS (
  -- Get various rules from segments for org enablement
    SELECT
        segment_rule_id AS rule_id,
        attribute,
        op,
        negate,
        FROM_JSON(values, 'array<string>') AS value_array
    FROM datamodel_launchdarkly_bronze.rule_clause
    WHERE
        segment_key IN (SELECT segment_key FROM relevant_segments)
        AND attribute IN ('orgId', 'orgCreatedAt', 'orgLocale', 'orgReleaseType', 'samNumber', 'orgIsInternal')
        AND date = (
            SELECT MAX(date) FROM datamodel_launchdarkly_bronze.rule_clause
            WHERE date <= GREATEST('{PARTITION_START}', '{INITIAL_LAUNCHDARKLY_DATE}')
        )
),
relevant_rules AS (
    -- Get all relevant rules for the FF
    SELECT *
    FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule
    WHERE
        feature_flag_key = '{flag_name}'
        AND date = (
            SELECT MAX(date) FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule
            WHERE date <= GREATEST('{PARTITION_START}', '{INITIAL_LAUNCHDARKLY_DATE}')
        )
),
rule_clauses_parsed AS (
    -- Parse to find all orgs that are also enabled from FF rules
    SELECT
        rc.feature_flag_environment_rule_id AS rule_id,
        rc.attribute,
        rc.op,
        rc.negate,
        LOWER(v.value) AS variation,
        FROM_JSON(rc.values, 'array<string>') AS value_array
    FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause rc
    JOIN relevant_rules ffr
        ON ffr.id = rc.feature_flag_environment_rule_id
    JOIN variations v
        ON ffr.variation = v.variation_index
    WHERE
        rc.attribute IN ('orgId', 'orgCreatedAt', 'orgLocale', 'orgReleaseType', 'samNumber', 'orgIsInternal')
        AND rc.date = (
            SELECT MAX(date) FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause
            WHERE date <= GREATEST('{PARTITION_START}', '{INITIAL_LAUNCHDARKLY_DATE}')
        )
),
launchdarkly_orgs AS (
    SELECT DISTINCT
        rule_id,
        attribute,
        'true' AS variation,
        op,
        negate,
        value
    FROM segment_parsing
    LATERAL VIEW EXPLODE(value_array) AS value

    UNION

    SELECT DISTINCT
        rule_id,
        attribute,
        variation,
        op,
        negate,
        value
    FROM rule_clauses_parsed
    LATERAL VIEW EXPLODE(value_array) AS value
),
us_low_bridge_strike_orgs AS (
    SELECT
        o.org_id,
        org_name,
        CASE
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) IS NULL THEN 'Unknown'
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 0 AND 1000 THEN '0 - 1k'
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 1000.01 AND 10000 THEN '1k - 10k'
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 10000.01 AND 100000 THEN '10k - 100k'
            ELSE COALESCE(a.customer_arr_segment, a_sam.customer_arr_segment)
        END AS account_arr_segment,
        COALESCE(o.account_size_segment_name, 'Unknown') AS account_size_segment_name,
        {ACCOUNT_BILLING_COUNTRY}
        COALESCE(o.sam_number, 'No SAM Number') AS sam_number,
        COALESCE(o.account_id, 'No Account ID') AS account_id,
        COALESCE(o.account_name, 'No Account Name') AS account_name,
        c.org_category,
        'us-west-2' AS region
    FROM datamodel_core.dim_organizations o
    JOIN product_analytics_staging.dim_organizations_classification c
        ON o.date = c.date
        AND o.org_id = c.org_id
    JOIN launchdarkly_orgs ldo
        ON ldo.attribute = 'orgId'
        AND ldo.value = CAST(o.org_id AS STRING)
        AND ldo.op = 'in'
        AND ldo.negate = FALSE
        AND LOWER(ldo.variation) = 'true'
    JOIN relevant_licenses_final rlf
        ON o.org_id = rlf.org_id
    LEFT OUTER JOIN account_arr a
        ON o.account_id = a.account_id
    LEFT OUTER JOIN account_arr_sam a_sam
        ON o.sam_number = a_sam.sam_number
    LEFT OUTER JOIN pubsec_us p
        ON o.sam_number = p.sam_number
    WHERE
        o.date = '{PARTITION_START}'
        AND o.locale IN ('us')
        AND o.release_type IN ('Early Adopter', 'Phase 1')

    UNION ALL

    SELECT
        o.org_id,
        org_name,
        CASE
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) IS NULL THEN 'Unknown'
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 0 AND 1000 THEN '0 - 1k'
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 1000.01 AND 10000 THEN '1k - 10k'
            WHEN COALESCE(a.customer_arr, a_sam.customer_arr) BETWEEN 10000.01 AND 100000 THEN '10k - 100k'
            ELSE COALESCE(a.customer_arr_segment, a_sam.customer_arr_segment)
        END AS account_arr_segment,
        COALESCE(o.account_size_segment_name, 'Unknown') AS account_size_segment_name,
        {ACCOUNT_BILLING_COUNTRY}
        COALESCE(o.sam_number, 'No SAM Number') AS sam_number,
        COALESCE(o.account_id, 'No Account ID') AS account_id,
        COALESCE(o.account_name, 'No Account Name') AS account_name,
        c.org_category,
        'eu-west-1' AS region
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
    JOIN data_tools_delta_share.product_analytics_staging.dim_organizations_classification c
        ON o.date = c.date
        AND o.org_id = c.org_id
    JOIN launchdarkly_orgs ldo
        ON ldo.attribute = 'orgId'
        AND ldo.value = CAST(o.org_id AS STRING)
        AND ldo.op = 'in'
        AND ldo.negate = FALSE
        AND LOWER(ldo.variation) = 'true'
    JOIN relevant_licenses_final rlf
        ON o.org_id = rlf.org_id
    LEFT OUTER JOIN account_arr a
        ON o.account_id = a.account_id
    LEFT OUTER JOIN account_arr_sam a_sam
        ON o.sam_number = a_sam.sam_number
    LEFT OUTER JOIN pubsec_eu p
        ON o.sam_number = p.sam_number
    WHERE
        o.date = '{PARTITION_START}'
        AND o.locale IN ('us')
        AND o.release_type IN ('Early Adopter', 'Phase 1')
),
relevant_orgs_final AS (
    SELECT *
    FROM final_relevant_orgs

    UNION ALL

    SELECT *
    FROM us_low_bridge_strike_orgs
),
devices_with_height AS (
    --Find all orgs where at least one asset has their vehicle height set
    SELECT DISTINCT org_id
    FROM datamodel_core_bronze.raw_productdb_devices
    WHERE
        date = '{PARTITION_START}'
        AND device_settings_proto.dimensions.height_meters IS NOT NULL

    UNION ALL

    SELECT DISTINCT org_id
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core_bronze.db/raw_productdb_devices`
    WHERE
        date = '{PARTITION_START}'
        AND device_settings_proto.dimensions.height_meters IS NOT NULL
),
{USAGE_SQL}
-- Definitions:
-- usage_weekly: Usage in the last 7 days (enabled for low bridge strike)
-- usage_monthly: Usage in the last 28 days (enabled for low bridge strike)
-- usage_prior_month: Usage in the 28 days prior to the last 28 days (enabled for low bridge strike)
-- usage_weekly_prior_month: Usage in the 7 days prior to the last 28 days (enabled for low bridge strike)
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
FROM relevant_orgs_final o
JOIN devices_with_height d
    ON o.org_id = d.org_id
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
        table_desc="""(Global) As part of Executive Scorecard, this shows usage stats for the Low Bridge Strike feature""",
        row_meaning="""Each row represents an org's usage of Low Bridge Strike over various time periods""",
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
        NonEmptyDQCheck(name="dq_non_empty_agg_executive_scorecard_low_bridge_strike_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_executive_scorecard_low_bridge_strike_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_executive_scorecard_low_bridge_strike_global",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        TrendDQCheck(
            name="dq_trend_agg_executive_scorecard_low_bridge_strike_global_enablement",
            lookback_days=1,
            tolerance=0.05,
            block_before_write=True,
        ),
    ],
    backfill_batch_size=1,
    upstreams=[
        "us-west-2:product_analytics_staging.stg_customer_enriched",
        "us-west-2|eu-west-1:dispatchdb_shards.dispatch_org_settings",
        "us-west-2|eu-west-1:datamodel_core_bronze.raw_productdb_devices",
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1:product_analytics_staging.dim_organizations_classification",
        "us-west-2|eu-west-1:finopsdb.customer_info",
        "us-west-2|eu-west-1:clouddb.organizations",
        "us-west-2:datamodel_launchdarkly_bronze.segment_flag",
        "us-west-2:datamodel_launchdarkly_bronze.rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_variation",
        "us-west-2:release_management.ld_flag_variation_index_map",
    ],
)
def agg_executive_scorecard_low_bridge_strike_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_executive_scorecard_low_bridge_strike_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    json_config = load_product_usage_config("low_bridge_strike")
    relevant_orgs = get_final_relevant_orgs(json_config, PARTITION_START)
    usage_sql = get_final_usage_sql(json_config, PARTITION_START)
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        RELEVANT_ORGS=relevant_orgs,
        USAGE_SQL=usage_sql,
        INITIAL_LAUNCHDARKLY_DATE=INITIAL_LAUNCHDARKLY_DATE,
        ACCOUNT_BILLING_COUNTRY=ACCOUNT_BILLING_COUNTRY,
        flag_name="low-bridge-strike-enabled",
    )
    context.log.info(f"{query}")
    return query
