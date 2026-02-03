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
from dataweb.userpkgs.product_usage_utils import (
    get_enabled_users_sql,
    load_product_usage_config,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)


QUERY = """
WITH pubsec_us AS (
    -- This identifies whether an organization is PubSec or not
    -- Note that we only care about this for US accounts
    SELECT
        sam_number,
        sled_account
    FROM finopsdb.customer_info
),
pubsec_eu AS (
    -- This identifies whether an organization is PubSec or not
    -- Note that we only care about this for US accounts
    SELECT
        sam_number,
        sled_account
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-finopsdb/finopsdb/customer_info_v0`
),
account_arr AS (
    -- Handles edge case where multiple accounts with different ARRs exist for a given SAM
    SELECT
        account_id,
        customer_arr,
        customer_arr_segment
    FROM product_analytics_staging.stg_customer_enriched
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_customer_enriched)
),
account_arr_sam AS (
    -- Handles cases where there's no account_id match (take max for SAM instead)
    SELECT sam_number_undecorated AS sam_number,
    MAX_BY(customer_arr, customer_arr) AS customer_arr,
    MAX_BY(customer_arr_segment, customer_arr) AS customer_arr_segment
    FROM product_analytics_staging.stg_customer_enriched
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_customer_enriched)
    GROUP BY sam_number_undecorated
),
relevant_orgs AS (
    -- All orgs have access
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
    LEFT OUTER JOIN account_arr a
        ON o.account_id = a.account_id
    LEFT OUTER JOIN account_arr_sam a_sam
        ON o.sam_number = a_sam.sam_number
    LEFT OUTER JOIN pubsec_us p
        ON o.sam_number = p.sam_number
    WHERE
        o.date = '{PARTITION_START}'

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
    LEFT OUTER JOIN account_arr a
        ON o.account_id = a.account_id
    LEFT OUTER JOIN account_arr_sam a_sam
        ON o.sam_number = a_sam.sam_number
    LEFT OUTER JOIN pubsec_eu p
        ON o.sam_number = p.sam_number
    WHERE
        o.date = '{PARTITION_START}'
),
license_filter AS (
    -- Despite serving all customers, we want to make sure the org has at least one semi-active license of the core types
    SELECT DISTINCT o.org_id
    FROM edw.silver.fct_license_orders_daily_snapshot fo
    JOIN datamodel_core.dim_organizations o
        ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
    JOIN edw.silver.license_hw_sku_xref xref
        ON fo.product_sku = xref.license_sku
    WHERE
        o.date = '{PARTITION_START}'
        AND DATE(fo._run_dt) BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}'
        AND (fo.net_quantity > 0)
        AND (
            fo.product_sku LIKE 'LIC-AG%'
            OR fo.product_sku LIKE 'LIC-VG%'
            OR fo.product_sku LIKE 'LIC-CM%'
            OR fo.product_sku LIKE 'LIC-AT%'
            OR fo.product_sku LIKE 'LIC-TRLR%'
        )
        AND xref.is_core_license = TRUE
        AND fo.product_sku != 'LIC-VG-ASAT'

    UNION ALL

    SELECT DISTINCT o.org_id
    FROM edw.silver.fct_license_orders_daily_snapshot fo
    JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
        ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
    JOIN edw.silver.license_hw_sku_xref xref
        ON fo.product_sku = xref.license_sku
    WHERE
        o.date = '{PARTITION_START}'
        AND DATE(fo._run_dt) BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}'
        AND (fo.net_quantity > 0)
        AND (
            fo.product_sku LIKE 'LIC-AG%'
            OR fo.product_sku LIKE 'LIC-VG%'
            OR fo.product_sku LIKE 'LIC-CM%'
            OR fo.product_sku LIKE 'LIC-AT%'
            OR fo.product_sku LIKE 'LIC-TRLR%'
        )
        AND xref.is_core_license = TRUE
        AND fo.product_sku != 'LIC-VG-ASAT'
),
{ENABLED_USERS_SQL}
chat_opens AS (
    -- Usage is defined as all clicks on the AI Assistant icon
    -- We filter out any internal usage
    -- Join to enabled users to solve case of changing emails
    SELECT
        DATE_TRUNC('day', mix.mp_date) AS event_date,
        CAST(mix.orgid AS BIGINT) AS org_id,
        COALESCE(eu.user_id, mix.mp_user_id, mix.mp_device_id) AS user_id
    FROM mixpanel_samsara.ai_chat_inline_icon_click mix
    LEFT OUTER JOIN enabled_users eu
        ON COALESCE(mix.mp_user_id, mix.mp_device_id) = eu.email
        AND CAST(mix.orgid AS BIGINT) = eu.org_id
    WHERE
        DATE_TRUNC('day', mix.mp_date) BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsara.com'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsara.canary%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsara.forms.canary%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsaratest%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%@samsara%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%@samsara-service-account.com'

    UNION ALL

    SELECT
        DATE_TRUNC('day', mix.mp_date) AS event_date,
        CAST(mix.orgid AS BIGINT) AS org_id,
        COALESCE(eu.user_id, mix.mp_user_id, mix.mp_device_id) AS user_id
    FROM mixpanel_samsara.ai_chat_floating_open mix
    LEFT OUTER JOIN enabled_users eu
        ON COALESCE(mix.mp_user_id, mix.mp_device_id) = eu.email
        AND CAST(mix.orgid AS BIGINT) = eu.org_id
    WHERE
        DATE_TRUNC('day', mix.mp_date) BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsara.com'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsara.canary%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsara.forms.canary%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsaratest%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%@samsara%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%@samsara-service-account.com'
),
traces AS (
    -- Usage is defined as all chat sends from the AI feature
    -- We filter out any internal usage
    -- Join to enabled users to solve case of changing emails
    SELECT
        DATE_TRUNC('day', mix.mp_date) AS event_date,
        CAST(mix.orgid AS BIGINT) AS org_id,
        COALESCE(eu.user_id, mix.mp_user_id, mix.mp_device_id) AS user_id
    FROM mixpanel_samsara.ai_chat_send mix
    LEFT OUTER JOIN enabled_users eu
        ON COALESCE(mix.mp_user_id, mix.mp_device_id) = eu.email
        AND CAST(mix.orgid AS BIGINT) = eu.org_id
    JOIN chat_opens co
        ON DATE_TRUNC('day', mix.mp_date) = co.event_date
        AND CAST(mix.orgid AS BIGINT) = co.org_id
        AND COALESCE(eu.user_id, mix.mp_user_id, mix.mp_device_id) = co.user_id
    WHERE
        DATE_TRUNC('day', mix.mp_date) BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsara.com'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsara.canary%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsara.forms.canary%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsaracanarydevcontractor%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%samsaratest%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%@samsara%'
        AND COALESCE(mix.mp_user_id, mix.mp_device_id) NOT LIKE '%@samsara-service-account.com'
),
traces_aggregated AS (
    SELECT
        org_id,
        COUNT(CASE WHEN event_date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN 1 END) AS usage_weekly,
        COUNT(CASE WHEN event_date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN 1 END) AS usage_monthly,
        COUNT(CASE WHEN event_date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND DATE_SUB('{PARTITION_START}', 28) THEN 1 END) AS usage_prior_month,
        COUNT(CASE WHEN event_date BETWEEN DATE_SUB('{PARTITION_START}', 34) AND DATE_SUB('{PARTITION_START}', 28) THEN 1 END) AS usage_weekly_prior_month,
        COUNT(DISTINCT CASE WHEN event_date = '{PARTITION_START}' THEN user_id END) AS daily_active_user,
        COUNT(DISTINCT CASE WHEN event_date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN user_id END) AS weekly_active_users,
        COUNT(DISTINCT CASE WHEN event_date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN user_id END) AS monthly_active_users,
        MAX(CASE WHEN event_date = '{PARTITION_START}' THEN 1 ELSE 0 END) AS org_active_day
    FROM traces
    GROUP BY org_id
),
relevant_driver_segments AS (
    -- Get any segments for the FF
    SELECT *
    FROM datamodel_launchdarkly_bronze.segment_flag
    WHERE
        key = '{driver_flag_name}'
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
        ffv.feature_flag_key = '{driver_flag_name}'
        AND ffv.date = (
            SELECT MAX(date) FROM datamodel_launchdarkly_bronze.feature_flag_variation
            WHERE date <= GREATEST('{PARTITION_START}', '{INITIAL_LAUNCHDARKLY_DATE}')
        )
),
segment_driver_parsing AS (
  -- Get all enabled orgs/drivers from segments
    SELECT attribute, FROM_JSON(values, 'array<string>') AS value_array
    FROM datamodel_launchdarkly_bronze.rule_clause
    WHERE
        segment_key IN (SELECT segment_key FROM relevant_driver_segments)
        AND attribute IN ('orgId', 'driverId')
        AND date = (
            SELECT MAX(date) FROM datamodel_launchdarkly_bronze.rule_clause
            WHERE date <= GREATEST('{PARTITION_START}', '{INITIAL_LAUNCHDARKLY_DATE}')
        )
),
relevant_driver_rules AS (
    -- Get all relevant rules for the FF
    SELECT *
    FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule
    WHERE
        feature_flag_key = '{driver_flag_name}'
        AND date = (
            SELECT MAX(date) FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule
            WHERE date <= GREATEST('{PARTITION_START}', '{INITIAL_LAUNCHDARKLY_DATE}')
        )
),
rule_clauses_driver_parsed AS (
    -- Parse to find all orgs/drivers that are also enabled
    SELECT
        rc.attribute,
        LOWER(v.value) AS variation,
        FROM_JSON(rc.values, 'array<string>') AS value_array
    FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause rc
    JOIN relevant_driver_rules ffr
        ON ffr.id = rc.feature_flag_environment_rule_id
    JOIN variations v
        ON ffr.variation = v.variation_index
    WHERE
        rc.attribute IN ('orgId', 'driverId')
        AND rc.date = (
            SELECT MAX(date) FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause
            WHERE date <= GREATEST('{PARTITION_START}', '{INITIAL_LAUNCHDARKLY_DATE}')
        )
),
launchdarkly_driver_orgs AS (
    SELECT DISTINCT value AS org_id
    FROM segment_driver_parsing
    LATERAL VIEW EXPLODE(value_array) AS value
    WHERE attribute = 'orgId'

    UNION

    SELECT DISTINCT value AS org_id
    FROM rule_clauses_driver_parsed
    LATERAL VIEW EXPLODE(value_array) AS value
    WHERE
        LOWER(variation) = 'true'
        AND attribute = 'orgId'
),
launchdarkly_driver_drivers AS (
    SELECT DISTINCT value AS driver_id
    FROM segment_driver_parsing
    LATERAL VIEW EXPLODE(value_array) AS value
    WHERE attribute = 'driverId'

    UNION

    SELECT DISTINCT value AS driver_id
    FROM rule_clauses_driver_parsed
    LATERAL VIEW EXPLODE(value_array) AS value
    WHERE LOWER(variation) = 'true'
    AND attribute = 'driverId'
),
enabled_drivers_us AS (
    -- Get all drivers for relevant orgs
    SELECT
      dd.org_id AS org_id,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= '{PARTITION_START}' THEN dd.driver_id END) AS daily_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 6) THEN dd.driver_id END) AS weekly_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27) THEN dd.driver_id END) AS monthly_enabled_drivers
    FROM datamodel_platform.dim_drivers dd
    WHERE
        dd.date = '{PARTITION_START}'
        AND (
            dd.org_id IN (SELECT CAST(org_id AS BIGINT) FROM launchdarkly_driver_orgs)
            OR dd.driver_id IN (SELECT CAST(driver_id AS BIGINT) FROM launchdarkly_driver_drivers)
        ) -- Filter for orgs or drivers
        AND (DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27))
    GROUP BY dd.org_id
),
enabled_drivers_eu AS (
    -- Get all drivers for relevant orgs
    SELECT
      dd.org_id AS org_id,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= '{PARTITION_START}' THEN dd.driver_id END) AS daily_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 6) THEN dd.driver_id END) AS weekly_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27) THEN dd.driver_id END) AS monthly_enabled_drivers
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers` dd
    WHERE
        dd.date = '{PARTITION_START}'
        AND (
            dd.org_id IN (SELECT CAST(org_id AS BIGINT) FROM launchdarkly_driver_orgs)
            OR dd.driver_id IN (SELECT CAST(driver_id AS BIGINT) FROM launchdarkly_driver_drivers)
        ) -- Filter for orgs or drivers
        AND (DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27))
    GROUP BY dd.org_id
),
enabled_drivers AS (
    SELECT * FROM enabled_drivers_us

    UNION ALL

    SELECT * FROM enabled_drivers_eu
)
-- Definitions:
-- usage_monthly: Usage in the last 7 days (message sends)
-- usage_weekly: Usage in the last 28 days (message sends)
-- usage_prior_month: Usage in the 28 days prior to the last 28 days (message sends)
-- usage_weekly_prior_month: Usage in the 7 days prior to the last 28 days (message sends)
-- daily_active_user: Number of unique users using the feature on the day in question (in this context, sending a message)
-- daily_enabled_users: Number of unique users who had access to the feature on the day in question
-- weekly_active_users: Number of unique users using the feature in the last 7 days (in this context, sending a message)
-- weekly_enabled_users: Number of unique users who had access to the feature in the last 7 days
-- monthly_active_users: Number of unique users using the feature in the last 28 days (in this context, sending a message)
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
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= '{PARTITION_START}' THEN eu.user_id END), 0) + MAX(COALESCE(ed.daily_enabled_drivers, 0)) AS daily_enabled_users,
    COALESCE(t.weekly_active_users, 0) AS weekly_active_users,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= DATE_SUB('{PARTITION_START}', 6) THEN eu.user_id END), 0) + MAX(COALESCE(ed.weekly_enabled_drivers, 0)) AS weekly_enabled_users,
    COALESCE(t.monthly_active_users, 0) AS monthly_active_users,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= DATE_SUB('{PARTITION_START}', 27) THEN eu.user_id END), 0) + MAX(COALESCE(ed.monthly_enabled_drivers, 0)) AS monthly_enabled_users,
    COALESCE(t.org_active_day, 0) AS org_active_day,
    CASE WHEN usage_weekly > 0 THEN 1 ELSE 0 END AS org_active_week,
    CASE WHEN usage_weekly_prior_month > 0 THEN 1 ELSE 0 END AS org_active_week_prior_month,
    CASE WHEN usage_monthly > 0 THEN 1 ELSE 0 END AS org_active_month,
    CASE WHEN usage_prior_month > 0 THEN 1 ELSE 0 END AS org_active_prior_month
FROM relevant_orgs ga
JOIN license_filter lf
    ON ga.org_id = lf.org_id
LEFT OUTER JOIN traces_aggregated t
    ON t.org_id = ga.org_id
LEFT OUTER JOIN enabled_users eu
    ON ga.org_id = eu.org_id
LEFT OUTER JOIN enabled_drivers ed
    ON ga.org_id = ed.org_id
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
        table_desc="""(Global) As part of Executive Scorecard, this shows usage stats for sending a message to AI Assistant from the main tooltips""",
        row_meaning="""Each row represents an org's usage of sending a message to AI Assistant from the main tooltips over various time periods""",
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
        NonEmptyDQCheck(name="dq_non_empty_agg_executive_scorecard_ai_assistant_main_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_executive_scorecard_ai_assistant_main_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_executive_scorecard_ai_assistant_main_global",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_ai_assistant_main_global_enabled_users",
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
            name="dq_sql_agg_executive_scorecard_ai_assistant_main_global_usage",
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
            name="dq_sql_agg_executive_scorecard_ai_assistant_main_global_high_usage",
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
            name="dq_sql_agg_executive_scorecard_ai_assistant_main_global_high_active_users",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE weekly_active_users > 10000
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        TrendDQCheck(name="dq_trend_agg_executive_scorecard_ai_assistant_main_global_enablement", lookback_days=1, tolerance=0.02),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_ai_assistant_main_global_usage_change",
            sql_query="""
                WITH today_usage AS (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS today_count
                    FROM df
                ),
                last_week_usage as (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS last_week_count
                    FROM product_analytics_staging.agg_executive_scorecard_ai_assistant_main_global
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
        "us-west-2:mixpanel_samsara.ai_chat_send",
        "us-west-2:mixpanel_samsara.ai_chat_inline_icon_click",
        "us-west-2:mixpanel_samsara.ai_chat_floating_open",
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1:datamodel_platform.dim_drivers",
        "us-west-2|eu-west-1:product_analytics_staging.dim_organizations_classification",
        "us-west-2|eu-west-1:datamodel_platform_bronze.raw_clouddb_users_organizations",
        "us-west-2|eu-west-1:datamodel_platform_bronze.raw_clouddb_users",
        "us-west-2|eu-west-1:datamodel_platform.dim_users_organizations_permissions",
        "us-west-2:datamodel_launchdarkly_bronze.segment_flag",
        "us-west-2:datamodel_launchdarkly_bronze.rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_variation",
        "us-west-2:release_management.ld_flag_variation_index_map",
        "us-west-2|eu-west-1:finopsdb.customer_info",
    ],
)
def agg_executive_scorecard_ai_assistant_main_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_executive_scorecard_ai_assistant_main_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    json_config = load_product_usage_config("ai_assistant_main")
    enabled_users_sql = get_enabled_users_sql(json_config, PARTITION_START)
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        driver_flag_name="driver-has-owl-chat-enabled",
        INITIAL_LAUNCHDARKLY_DATE=INITIAL_LAUNCHDARKLY_DATE,
        ACCOUNT_BILLING_COUNTRY=ACCOUNT_BILLING_COUNTRY,
        ENABLED_USERS_SQL=enabled_users_sql,
    )
    context.log.info(f"{query}")
    return query
