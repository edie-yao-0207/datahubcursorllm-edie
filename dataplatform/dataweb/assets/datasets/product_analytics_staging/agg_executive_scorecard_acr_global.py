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
ga_status_orgs AS (
    -- All orgs have access now
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
        AND fo.net_quantity > 0
        AND (
            fo.product_sku LIKE 'LIC-AG%'
            OR fo.product_sku LIKE 'LIC-VG%'
            OR fo.product_sku LIKE 'LIC-CM%'
            OR fo.product_sku LIKE 'LIC-AT%'
            OR fo.product_sku LIKE 'LIC-TRLR%'
        )
        AND xref.is_core_license
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
        AND fo.net_quantity > 0
        AND (
            fo.product_sku LIKE 'LIC-AG%'
            OR fo.product_sku LIKE 'LIC-VG%'
            OR fo.product_sku LIKE 'LIC-CM%'
            OR fo.product_sku LIKE 'LIC-AT%'
            OR fo.product_sku LIKE 'LIC-TRLR%'
        )
        AND xref.is_core_license
        AND fo.product_sku != 'LIC-VG-ASAT'
),
relevant_licenses AS (
    -- Valid licenses for ACR include:
    -- 1. Legacy licenses
    -- 2. CM-only SKUs
    -- 3. Various VG SKUs
    -- 4. All Platform Premier SKUs
    -- Only active before 8/18/25
    SELECT DISTINCT o.org_id
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
            fo.product_sku IN ('LIC-CM-ANLG', 'LIC-CM-ENT', 'LIC-CM1-ENT', 'LIC-CM1-EXPRESS', 'LIC-CM2-ENT', 'LIC-CM2-EXPRESS', 'LIC-CM25-ENT', 'LIC-NVR10', 'LIC-NVR10-2', 'LIC-NVR10-3', 'LIC-NVR10-6') -- Legacy CM licenses
            OR fo.product_sku IN ('LIC-VG-PREMIER', 'LIC-VG-PREMIER-PS') -- VG Premier licenses
            OR fo.product_sku IN ('LIC-VG-ENTERPRISE') -- VG Enterprise license
            OR fo.product_sku LIKE '%PLTFM-PREM' -- Platform Premier licenses
            OR (xref.is_legacy_license AND xref.is_core_license)
        )

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
        AND fo.net_quantity > 0
        AND (
            fo.product_sku IN ('LIC-CM-ANLG', 'LIC-CM-ENT', 'LIC-CM1-ENT', 'LIC-CM1-EXPRESS', 'LIC-CM2-ENT', 'LIC-CM2-EXPRESS', 'LIC-CM25-ENT', 'LIC-NVR10', 'LIC-NVR10-2', 'LIC-NVR10-3', 'LIC-NVR10-6') -- Legacy CM licenses
            OR fo.product_sku IN ('LIC-VG-PREMIER', 'LIC-VG-PREMIER-PS') -- VG Premier licenses
            OR fo.product_sku IN ('LIC-VG-ENTERPRISE') -- VG Enterprise license
            OR fo.product_sku LIKE '%PLTFM-PREM' -- Platform Premier licenses
            OR (xref.is_legacy_license AND xref.is_core_license)
        )
),
users_us AS (
    -- Filter out internal/canary users
    SELECT DISTINCT
      user_id
    FROM datamodel_platform.dim_users_organizations
    WHERE
        date = '{PARTITION_START}'
        AND email NOT LIKE '%@samsara.com'
        AND email NOT LIKE '%samsara.canary%'
        AND email NOT LIKE '%samsara.forms.canary%'
        AND email NOT LIKE '%samsaracanarydevcontractor%'
        AND email NOT LIKE '%samsaratest%'
        AND email NOT LIKE '%@samsara%'
        AND email NOT LIKE '%@samsara-service-account.com'
),
users_eu AS (
    -- Filter out internal/canary users
    SELECT DISTINCT
      user_id
    FROM data_tools_delta_share.datamodel_platform.dim_users_organizations
    WHERE
        date = '{PARTITION_START}'
        AND email NOT LIKE '%@samsara.com'
        AND email NOT LIKE '%samsara.canary%'
        AND email NOT LIKE '%samsara.forms.canary%'
        AND email NOT LIKE '%samsaracanarydevcontractor%'
        AND email NOT LIKE '%samsaratest%'
        AND email NOT LIKE '%@samsara%'
        AND email NOT LIKE '%@samsara-service-account.com'
),
events_us AS (
    -- Usage is defined as any report run call for a non-internal user
    SELECT
        CAST(rr.org_id AS BIGINT) AS org_id,
        DATE_TRUNC('day', rr.created_at) AS event_date,
        rr.user_id AS user_id,
        CONCAT(rr.org_id, '_', rr.uuid) AS usage_id
    FROM reportconfigdb_shards.report_runs rr
    JOIN clouddb.organizations o
        ON rr.org_id = o.id
    JOIN users_us u
        ON rr.user_id = u.user_id
    WHERE
        rr.report_run_metadata.report_config IS NOT NULL
        AND rr.report_run_metadata.report_config.is_superuser_only = FALSE -- Exclude superuser-only reports
        AND rr.org_id IS NOT NULL
        AND rr.created_at IS NOT NULL
        AND DATE_TRUNC('day', rr.created_at) BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        -- Note: We are not filtering on rr.status (e.g., status=2 for 'Complete')
        -- to count all attempts that meet the above criteria, similar to the engineer's query.
        -- If only 'Complete' runs should be counted, add 'AND rr.status = 2' here.
),
events_eu AS (
    -- Usage is defined as any report run call for a non-internal user
    SELECT
        CAST(rr.org_id AS BIGINT) AS org_id,
        DATE_TRUNC('day', rr.created_at) AS event_date,
        rr.user_id AS user_id,
        CONCAT(rr.org_id, '_', rr.uuid) AS usage_id
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-reportconfigdb/reportconfigdb/report_runs_v0` rr
    JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` o
        ON rr.org_id = o.id
    JOIN users_eu u
        ON rr.user_id = u.user_id
    WHERE
        rr.report_run_metadata.report_config IS NOT NULL
        AND rr.report_run_metadata.report_config.is_superuser_only = FALSE -- Exclude superuser-only reports
        AND rr.org_id IS NOT NULL
        AND rr.created_at IS NOT NULL
        AND DATE_TRUNC('day', rr.created_at) BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        -- Note: We are not filtering on rr.status (e.g., status=2 for 'Complete')
        -- to count all attempts that meet the above criteria, similar to the engineer's query.
        -- If only 'Complete' runs should be counted, add 'AND rr.status = 2' here.

    UNION ALL

    SELECT
        CAST(rr.org_id AS BIGINT) AS org_id,
        DATE_TRUNC('day', rr.created_at) AS event_date,
        rr.user_id AS user_id,
        CONCAT(rr.org_id, '_', rr.uuid) AS usage_id
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-reportconfig-shard-1db/reportconfigdb/report_runs_v0` rr
    JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/organizations_v3` o
        ON rr.org_id = o.id
    JOIN users_eu u
        ON rr.user_id = u.user_id
    WHERE
        rr.report_run_metadata.report_config IS NOT NULL
        AND rr.report_run_metadata.report_config.is_superuser_only = FALSE -- Exclude superuser-only reports
        AND rr.org_id IS NOT NULL
        AND rr.created_at IS NOT NULL
        AND DATE_TRUNC('day', rr.created_at) BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        -- Note: We are not filtering on rr.status (e.g., status=2 for 'Complete')
        -- to count all attempts that meet the above criteria, similar to the engineer's query.
        -- If only 'Complete' runs should be counted, add 'AND rr.status = 2' here.
),
{ENABLED_USERS_SQL}
events AS (
    SELECT * FROM events_us

    UNION ALL

    SELECT * FROM events_eu
)
-- Definitions:
-- usage_weekly: Usage in the last 7 days (report runs)
-- usage_monthly: Usage in the last 28 days (report runs)
-- usage_prior_month: Usage in the 28 days prior to the last 28 days (report runs)
-- usage_weekly_prior_month: Usage in the 7 days prior to the last 28 days (report runs)
-- daily_active_user: Number of unique users using the feature on the day in question (in this context, running a report)
-- daily_enabled_users: Number of unique users who had access to the feature on the day in question
-- weekly_active_users: Number of unique users using the feature in the last 7 days (in this context, running a report)
-- weekly_enabled_users: Number of unique users who had access to the feature in the last 7 days
-- monthly_active_users: Number of unique users using the feature in the last 28 days (in this context, running a report)
-- monthly_enabled_users: Number of unique users who had access to the feature in the last 28 days
-- org_active_day: Whether the org used the feature on the day in question
-- org_active_week: Whether the org used the feature in the last 7 days
-- org_active_week_prior_month: Whether the org used the feature in the 7 days prior to the last 28 days
-- org_active_month: Whether the org used the feature in the last 28 days
-- org_active_prior_month: Whether the org used the feature in the 28 days prior to the last 28 days
-- Only apply license join from before GA date
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
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN e.usage_id END), 0) AS usage_weekly,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN e.usage_id END), 0) AS usage_monthly,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND DATE_SUB('{PARTITION_START}', 28) THEN e.usage_id END), 0) AS usage_prior_month,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 34) AND DATE_SUB('{PARTITION_START}', 28) THEN e.usage_id END), 0) AS usage_weekly_prior_month,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date = '{PARTITION_START}' THEN e.user_id END), 0) AS daily_active_user,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= '{PARTITION_START}' THEN eu.user_id END), 0) AS daily_enabled_users,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN e.user_id END), 0) AS weekly_active_users,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= DATE_SUB('{PARTITION_START}', 6) THEN eu.user_id END), 0) AS weekly_enabled_users,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN e.user_id END), 0) AS monthly_active_users,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= DATE_SUB('{PARTITION_START}', 27) THEN eu.user_id END), 0) AS monthly_enabled_users,
    MAX(CASE WHEN e.event_date = '{PARTITION_START}' THEN 1 ELSE 0 END) AS org_active_day,
    CASE WHEN usage_weekly > 0 THEN 1 ELSE 0 END AS org_active_week,
    CASE WHEN usage_weekly_prior_month > 0 THEN 1 ELSE 0 END AS org_active_week_prior_month,
    CASE WHEN usage_monthly > 0 THEN 1 ELSE 0 END AS org_active_month,
    CASE WHEN usage_prior_month > 0 THEN 1 ELSE 0 END AS org_active_prior_month
FROM ga_status_orgs ga
JOIN relevant_licenses rl
    ON ga.org_id = rl.org_id
LEFT OUTER JOIN events e
    ON ga.org_id = e.org_id
LEFT OUTER JOIN enabled_users eu
    ON ga.org_id = eu.org_id
WHERE '{PARTITION_START}' < '{GA_DATE}'
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
    ga.account_name

UNION ALL

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
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN e.usage_id END), 0) AS usage_weekly,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN e.usage_id END), 0) AS usage_monthly,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND DATE_SUB('{PARTITION_START}', 28) THEN e.usage_id END), 0) AS usage_prior_month,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 34) AND DATE_SUB('{PARTITION_START}', 28) THEN e.usage_id END), 0) AS usage_weekly_prior_month,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date = '{PARTITION_START}' THEN e.user_id END), 0) AS daily_active_user,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= '{PARTITION_START}' THEN eu.user_id END), 0) AS daily_enabled_users,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN e.user_id END), 0) AS weekly_active_users,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= DATE_SUB('{PARTITION_START}', 6) THEN eu.user_id END), 0) AS weekly_enabled_users,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN e.user_id END), 0) AS monthly_active_users,
    COALESCE(COUNT(DISTINCT CASE WHEN eu.expire_at IS NULL OR DATE(eu.expire_at) >= DATE_SUB('{PARTITION_START}', 27) THEN eu.user_id END), 0) AS monthly_enabled_users,
    MAX(CASE WHEN e.event_date = '{PARTITION_START}' THEN 1 ELSE 0 END) AS org_active_day,
    CASE WHEN usage_weekly > 0 THEN 1 ELSE 0 END AS org_active_week,
    CASE WHEN usage_weekly_prior_month > 0 THEN 1 ELSE 0 END AS org_active_week_prior_month,
    CASE WHEN usage_monthly > 0 THEN 1 ELSE 0 END AS org_active_month,
    CASE WHEN usage_prior_month > 0 THEN 1 ELSE 0 END AS org_active_prior_month
FROM ga_status_orgs ga
JOIN license_filter lf
    ON ga.org_id = lf.org_id
LEFT OUTER JOIN events e
    ON ga.org_id = e.org_id
LEFT OUTER JOIN enabled_users eu
    ON ga.org_id = eu.org_id
WHERE '{PARTITION_START}' >= '{GA_DATE}'
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
    ga.account_name
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""(Global) As part of Executive Scorecard, this shows usage stats for the ACR feature""",
        row_meaning="""Each row represents an org's usage of ACR over various time periods""",
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
        NonEmptyDQCheck(name="dq_non_empty_agg_executive_scorecard_acr_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_executive_scorecard_acr_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_executive_scorecard_acr_global",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_acr_global_enabled_users",
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
            name="dq_sql_agg_executive_scorecard_acr_global_usage",
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
            name="dq_sql_agg_executive_scorecard_acr_global_high_usage",
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
            name="dq_sql_agg_executive_scorecard_acr_global_high_active_users",
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
            name="dq_trend_agg_executive_scorecard_acr_global_enablement",
            lookback_days=1,
            tolerance=0.05,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_acr_global_usage_change",
            sql_query="""
                WITH today_usage AS (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS today_count
                    FROM df
                ),
                last_week_usage as (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS last_week_count
                    FROM product_analytics_staging.agg_executive_scorecard_acr_global
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
        "us-west-2|eu-west-1:reportconfigdb_shards.report_runs",
        "us-west-2|eu-west-1:clouddb.organizations",
        "us-west-2|eu-west-1:datamodel_platform.dim_users_organizations",
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1:product_analytics_staging.dim_organizations_classification",
        "us-west-2|eu-west-1:datamodel_platform_bronze.raw_clouddb_users_organizations",
        "us-west-2|eu-west-1:datamodel_platform_bronze.raw_clouddb_users",
        "us-west-2|eu-west-1:datamodel_platform.dim_users_organizations_permissions",
        "us-west-2|eu-west-1:finopsdb.customer_info",
        "us-west-2:product_analytics_staging.stg_customer_enriched",
    ],
)
def agg_executive_scorecard_acr_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_executive_scorecard_acr_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    json_config = load_product_usage_config("acr")
    enabled_users_sql = get_enabled_users_sql(json_config, PARTITION_START)
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        GA_DATE='2025-08-19',
        ACCOUNT_BILLING_COUNTRY=ACCOUNT_BILLING_COUNTRY,
        ENABLED_USERS_SQL=enabled_users_sql,
    )
    context.log.info(f"{query}")
    return query
