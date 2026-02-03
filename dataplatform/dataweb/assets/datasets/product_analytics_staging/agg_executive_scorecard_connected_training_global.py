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
    -- No upfront org restriction, handled via licenses
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
relevant_licenses AS (
    -- LIC-TRAINING or LIC-FL-APPS are relevant licenses

    SELECT DISTINCT o.org_id, fo.product_sku AS sku
    FROM edw.silver.fct_license_orders_daily_snapshot fo
    JOIN datamodel_core.dim_organizations o
        ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
    JOIN edw.silver.license_hw_sku_xref xref
        ON fo.product_sku = xref.license_sku
    WHERE
        o.date = '{PARTITION_START}'
        AND DATE(fo._run_dt) BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}'
        AND fo.net_quantity > 0
        AND (
            fo.product_sku IN ('LIC-FL-APPS', 'LIC-TRAINING', 'LIC-TRAINING-PROMO')
        )

    UNION ALL

    SELECT DISTINCT o.org_id, fo.product_sku AS sku
    FROM edw.silver.fct_license_orders_daily_snapshot fo
    JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
        ON fo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
    JOIN edw.silver.license_hw_sku_xref xref
        ON fo.product_sku = xref.license_sku
    WHERE
        o.date = '{PARTITION_START}'
        AND DATE(fo._run_dt) BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}'
        AND fo.net_quantity > 0
        AND (
            fo.product_sku IN ('LIC-FL-APPS', 'LIC-TRAINING', 'LIC-TRAINING-PROMO')
        )
),
relevant_licenses_deduped AS (
    -- Dedup to avoid duplicate orgs with multiple applicable SKUs
    SELECT DISTINCT org_id
    FROM relevant_licenses
),
relevant_licenses_fl_apps AS (
    SELECT DISTINCT org_id
    FROM relevant_licenses
    WHERE sku = 'LIC-FL-APPS'
),
relevant_licenses_training AS (
    SELECT DISTINCT org_id
    FROM relevant_licenses
    WHERE sku IN ('LIC-TRAINING', 'LIC-TRAINING-PROMO')
),
users_us AS (
    -- Filter out internal users
    SELECT DISTINCT
      user_id
    FROM datamodel_platform.dim_users_organizations
    WHERE
        date = '{PARTITION_START}'
        AND (
            email LIKE '%@samsara.com'
            OR email LIKE '%samsara.canary%'
            OR email LIKE '%samsara.forms.canary%'
            OR email LIKE '%samsaracanarydevcontractor%'
            OR email LIKE '%samsaratest%'
            OR email LIKE '%@samsara%'
            OR email LIKE '%@samsara-service-account.com'
        )
),
users_eu AS (
    -- Filter out internal users
    SELECT DISTINCT
      user_id
    FROM data_tools_delta_share.datamodel_platform.dim_users_organizations
    WHERE
        date = '{PARTITION_START}'
        AND (
            email LIKE '%@samsara.com'
            OR email LIKE '%samsara.canary%'
            OR email LIKE '%samsara.forms.canary%'
            OR email LIKE '%samsaracanarydevcontractor%'
            OR email LIKE '%samsaratest%'
            OR email LIKE '%@samsara%'
            OR email LIKE '%@samsara-service-account.com'
        )
),
events_us AS (
    -- Usage is defined as orgs with at least one custom course assigned from a non-internal user
    -- We use assigned user as they're the one who is responsible for the training
    SELECT
        CAST(f.org_id AS BIGINT) AS org_id,
        DATE(f.assigned_at) AS event_date,
        f.assigned_to_polymorphic AS user_id
    FROM formsdb_shards.form_submissions f
    JOIN trainingdb_shards.courses c
        ON f.form_template_uuid = c.uuid
    JOIN trainingdb_shards.categories ca
        ON c.category_uuid = ca.uuid
    LEFT OUTER JOIN users_us u
        ON SPLIT_PART(f.assigned_to_polymorphic, '-', 2) = u.user_id
        AND SPLIT_PART(f.assigned_to_polymorphic, '-', 1) = 'user'
    WHERE
        f.assigned_at IS NOT NULL --assigned
        AND u.user_id IS NULL -- Exclude internal users
        AND COALESCE(DATE(f.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --record is still valid
        AND DATE(f.assigned_at) BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        AND f.product_type = 3 -- training
        AND (
            c.global_course_uuid IS NULL
            OR TRIM(ca.label) NOT IN (
                'Driver Safety - Shorts',
                'Samsara Driver App',
                'New Samsara Driver App',
                'Samsara Intro',
                'Announcements'
            )
        )
        AND COALESCE(DATE(FROM_UNIXTIME(c.deleted_at_ms / 1000)), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --record is still valid
),
form_submissions_eu AS (
    SELECT
        org_id,
        assigned_at,
        assigned_to_polymorphic,
        form_template_uuid,
        server_deleted_at,
        product_type
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-formsdb/formsdb/form_submissions_v0`

    UNION ALL

    SELECT
        org_id,
        assigned_at,
        assigned_to_polymorphic,
        form_template_uuid,
        server_deleted_at,
        product_type
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-forms-shard-1db/formsdb/form_submissions_v0`
),
courses_eu AS (
    SELECT
        uuid,
        category_uuid,
        global_course_uuid,
        deleted_at_ms,
        title
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-training-shard-1db/trainingdb/courses_v0`

    UNION ALL

    SELECT
        uuid,
        category_uuid,
        global_course_uuid,
        deleted_at_ms,
        title
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-trainingdb/trainingdb/courses_v0`
),
categories_eu AS (
    SELECT
        uuid,
        label
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-training-shard-1db/trainingdb/categories_v0`

    UNION ALL

    SELECT
        uuid,
        label
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-trainingdb/trainingdb/categories_v0`
),
events_eu AS (
    -- Usage is defined as orgs with at least one custom course assigned from a non-internal user
    -- We use assigned user as they're the one who is responsible for the training
    SELECT
        CAST(f.org_id AS BIGINT) AS org_id,
        DATE(f.assigned_at) AS event_date,
        f.assigned_to_polymorphic AS user_id
    FROM form_submissions_eu f
    JOIN courses_eu c
        ON f.form_template_uuid = c.uuid
    JOIN categories_eu ca
        ON c.category_uuid = ca.uuid
    LEFT OUTER JOIN users_eu u
        ON SPLIT_PART(f.assigned_to_polymorphic, '-', 2) = u.user_id
        AND SPLIT_PART(f.assigned_to_polymorphic, '-', 1) = 'user'
    WHERE
        u.user_id IS NULL -- Exclude internal users
        AND f.assigned_at IS NOT NULL --assigned
        AND COALESCE(DATE(f.server_deleted_at), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --valid record
        AND DATE(f.assigned_at) BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        AND product_type = 3 -- training
        AND (
            c.global_course_uuid IS NULL
            OR TRIM(ca.label) NOT IN (
                'Driver Safety - Shorts',
                'Samsara Driver App',
                'New Samsara Driver App',
                'Samsara Intro',
                'Announcements'
            )
        )
        AND COALESCE(DATE(FROM_UNIXTIME(c.deleted_at_ms / 1000)), DATE_ADD('{PARTITION_START}', 1)) > '{PARTITION_START}' --valid records
),
events AS (
    SELECT * FROM events_us

    UNION ALL

    SELECT * FROM events_eu
),
events_aggregated AS (
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
    FROM events
    GROUP BY org_id
),
enabled_license_assignment AS (
    -- For LIC-TRAINING/LIC-TRAINING-PROMO, you must be assigned training
    SELECT
      la.org_id AS org_id,
      COUNT(DISTINCT CASE WHEN DATE(la.end_time) IS NULL OR DATE(la.end_time) >= '{PARTITION_START}' THEN la.entity_id END) AS daily_enabled_users,
      COUNT(DISTINCT CASE WHEN DATE(la.end_time) IS NULL OR DATE(la.end_time) >= DATE_SUB('{PARTITION_START}', 6) THEN la.entity_id END) AS weekly_enabled_users,
      COUNT(DISTINCT CASE WHEN DATE(la.end_time) IS NULL OR DATE(la.end_time) >= DATE_SUB('{PARTITION_START}', 27) THEN la.entity_id END) AS monthly_enabled_users
    FROM datamodel_platform.dim_license_assignment la
    LEFT OUTER JOIN relevant_licenses_fl_apps rl
        ON la.org_id = rl.org_id
    WHERE
        la.date = '{PARTITION_START}'
        AND la.sku IN ('LIC-TRAINING', 'LIC-TRAINING-PROMO')
        AND rl.org_id IS NULL -- Only include orgs with TRAINING, not FL-APPS
        AND (la.end_time IS NULL OR DATE(la.end_time) >= DATE_SUB('{PARTITION_START}', 27))
    GROUP BY la.org_id

    UNION ALL

    SELECT
      la.org_id AS org_id,
      COUNT(DISTINCT CASE WHEN DATE(la.end_time) IS NULL OR DATE(la.end_time) >= '{PARTITION_START}' THEN la.entity_id END) AS daily_enabled_users,
      COUNT(DISTINCT CASE WHEN DATE(la.end_time) IS NULL OR DATE(la.end_time) >= DATE_SUB('{PARTITION_START}', 6) THEN la.entity_id END) AS weekly_enabled_users,
      COUNT(DISTINCT CASE WHEN DATE(la.end_time) IS NULL OR DATE(la.end_time) >= DATE_SUB('{PARTITION_START}', 27) THEN la.entity_id END) AS monthly_enabled_users
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_license_assignment` la
    LEFT OUTER JOIN relevant_licenses_fl_apps rl
        ON la.org_id = rl.org_id
    WHERE
        la.date = '{PARTITION_START}'
        AND la.sku IN ('LIC-TRAINING', 'LIC-TRAINING-PROMO')
        AND rl.org_id IS NULL -- Only include orgs with TRAINING, not FL-APPS
        AND (la.end_time IS NULL OR DATE(la.end_time) >= DATE_SUB('{PARTITION_START}', 27))
    GROUP BY la.org_id
),
enabled_drivers_us AS (
    -- We calculate drivers separately as it's more efficient
    -- This is for orgs with LIC-FL-APPS
    SELECT
      dd.org_id AS org_id,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= '{PARTITION_START}' THEN dd.driver_id END) AS daily_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 6) THEN dd.driver_id END) AS weekly_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27) THEN dd.driver_id END) AS monthly_enabled_drivers
    FROM datamodel_platform.dim_drivers dd
    JOIN relevant_licenses_fl_apps rl
        ON dd.org_id = rl.org_id
    WHERE
        dd.date = '{PARTITION_START}'
        AND (DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27))
    GROUP BY dd.org_id
),
enabled_drivers_eu AS (
    -- We calculate drivers separately as it's more efficient
    -- This is for orgs with LIC-FL-APPS
    SELECT
      dd.org_id AS org_id,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= '{PARTITION_START}' THEN dd.driver_id END) AS daily_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 6) THEN dd.driver_id END) AS weekly_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27) THEN dd.driver_id END) AS monthly_enabled_drivers
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers` dd
    JOIN relevant_licenses_fl_apps rl
        ON dd.org_id = rl.org_id
    WHERE
        dd.date = '{PARTITION_START}'
        AND (DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27))
    GROUP BY dd.org_id
),
enabled_drivers AS (
    SELECT * FROM enabled_drivers_us

    UNION ALL

    SELECT * FROM enabled_drivers_eu
)
-- Definitions:
-- usage_weekly: Usage in the last 7 days (training assignments)
-- usage_monthly: Usage in the last 28 days (training assignments)
-- usage_prior_month: Usage in the 28 days prior to the last 28 days (training assignments)
-- usage_weekly_prior_month: Usage in the 7 days prior to the last 28 days (training assignments)
-- daily_active_user: Number of unique users using the feature on the day in question (users who were assigned a training)
-- daily_enabled_users: Number of unique users who had access to the feature on the day in question
-- weekly_active_users: Number of unique users using the feature in the last 7 days (users who were assigned a training)
-- weekly_enabled_users: Number of unique users who had access to the feature in the last 7 days
-- monthly_active_users: Number of unique users using the feature in the last 28 days (users who were assigned a training)
-- monthly_enabled_users: Number of unique users who had access to the feature in the last 28 days
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
    COALESCE(MAX(ela.daily_enabled_users), 0) + COALESCE(MAX(ed.daily_enabled_drivers), 0) AS daily_enabled_users,
    COALESCE(e.weekly_active_users, 0) AS weekly_active_users,
    COALESCE(MAX(ela.weekly_enabled_users), 0) + COALESCE(MAX(ed.weekly_enabled_drivers), 0) AS weekly_enabled_users,
    COALESCE(e.monthly_active_users, 0) AS monthly_active_users,
    COALESCE(MAX(ela.monthly_enabled_users), 0) + COALESCE(MAX(ed.monthly_enabled_drivers), 0) AS monthly_enabled_users,
    COALESCE(e.org_active_day, 0) AS org_active_day,
    CASE WHEN usage_weekly > 0 THEN 1 ELSE 0 END AS org_active_week,
    CASE WHEN usage_weekly_prior_month > 0 THEN 1 ELSE 0 END AS org_active_week_prior_month,
    CASE WHEN usage_monthly > 0 THEN 1 ELSE 0 END AS org_active_month,
    CASE WHEN usage_prior_month > 0 THEN 1 ELSE 0 END AS org_active_prior_month
FROM relevant_orgs o
JOIN relevant_licenses_deduped rl
    ON o.org_id = rl.org_id
LEFT OUTER JOIN events_aggregated e
    ON o.org_id = e.org_id
LEFT OUTER JOIN enabled_license_assignment ela
    ON o.org_id = ela.org_id
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
        table_desc="""(Global) As part of Executive Scorecard, this shows usage stats for the Connected Training feature""",
        row_meaning="""Each row represents an org's usage of Connected Training over various time periods""",
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
        NonEmptyDQCheck(name="dq_non_empty_agg_executive_scorecard_connected_training_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_executive_scorecard_connected_training_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_executive_scorecard_connected_training_global",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_connected_training_global_enabled_users",
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
            name="dq_sql_agg_executive_scorecard_connected_training_global_usage",
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
            name="dq_sql_agg_executive_scorecard_connected_training_global_high_usage",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE usage_weekly > 25000
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_connected_training_global_high_active_users",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE weekly_active_users > 10000
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        TrendDQCheck(
            name="dq_trend_agg_executive_scorecard_connected_training_global_enablement",
            lookback_days=1,
            tolerance=0.05,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_connected_training_global_usage_change",
            sql_query="""
                WITH today_usage AS (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS today_count
                    FROM df
                ),
                last_week_usage as (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS last_week_count
                    FROM product_analytics_staging.agg_executive_scorecard_connected_training_global
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
        "us-west-2|eu-west-1:trainingdb_shards.courses",
        "us-west-2|eu-west-1:trainingdb_shards.categories",
        "us-west-2|eu-west-1:datamodel_platform.dim_users_organizations",
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1:datamodel_platform.dim_license_assignment",
        "us-west-2|eu-west-1:product_analytics_staging.dim_organizations_classification",
        "us-west-2|eu-west-1:datamodel_platform.dim_drivers",
        "us-west-2|eu-west-1:finopsdb.customer_info",
    ],
)
def agg_executive_scorecard_connected_training_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_executive_scorecard_connected_training_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        ACCOUNT_BILLING_COUNTRY=ACCOUNT_BILLING_COUNTRY,
    )
    context.log.info(f"{query}")
    return query
