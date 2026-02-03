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
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)


QUERY = """
WITH relevant_segments AS (
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
pivoted_conditions AS (
    -- Pivot to apply multiple conditions in the same rule if exists
    SELECT
        rule_id,
        variation,
        op,
        negate,
        COLLECT_SET(CASE WHEN attribute = 'orgId' THEN value ELSE NULL END) AS org_ids,
        COLLECT_SET(CASE WHEN attribute = 'orgLocale' THEN value ELSE NULL END) AS org_locales,
        COLLECT_SET(CASE WHEN attribute = 'orgReleaseType' THEN value ELSE NULL END) AS org_release_types,
        COLLECT_SET(CASE WHEN attribute = 'orgCreatedAt' THEN value ELSE NULL END) AS org_created_at,
        COLLECT_SET(CASE WHEN attribute = 'samNumber' THEN value ELSE NULL END) AS sam_numbers,
        COLLECT_SET(CASE WHEN attribute = 'orgIsInternal' THEN value ELSE NULL END) AS is_internal
    FROM launchdarkly_orgs
    GROUP BY rule_id, variation, op, negate
),
pivoted_conditions_exploded AS (
    SELECT
        rule_id,
        variation,
        op,
        negate,
        org_ids,
        org_locales,
        org_release_types,
        sam_numbers,
        is_internal,
        exploded_ts AS org_created_at_ts
    FROM (
        SELECT
            *,
            EXPLODE(
                CASE
                WHEN SIZE(org_created_at) = 0 OR org_created_at IS NULL THEN ARRAY(NULL)
                ELSE org_created_at
                END
            ) AS exploded_ts
        FROM pivoted_conditions
    ) pc
),
pubsec_us AS (
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
    -- Restrictions are handled via beta or license
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
        o.locale,
        o.created_at,
        o.release_type,
        'us-west-2' AS region
    FROM datamodel_core.dim_organizations o
    JOIN product_analytics_staging.dim_organizations_classification c
        ON o.date = c.date
        AND o.org_id = c.org_id
    LEFT OUTER JOIN account_arr a
        ON o.account_id = a.account_id
    LEFT OUTER JOIN account_arr_sam a_sam
        ON o.sam_number = a_sam.sam_number
    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
        ON (
            LOWER(ldo.variation) = 'false'
            AND (
                SIZE(ldo.org_ids) = 0
                OR (
                    ldo.negate = FALSE AND (
                        CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(o.org_id AS STRING))
                        WHEN ldo.op = 'lessThan' THEN o.org_id < CAST(ldo.org_ids[0] AS BIGINT)
                        WHEN ldo.op = 'greaterThan' THEN o.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(o.org_id AS STRING))
                        WHEN ldo.op = 'lessThan' THEN o.org_id < CAST(ldo.org_ids[0] AS BIGINT)
                        WHEN ldo.op = 'greaterThan' THEN o.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
                    )
                )
            )
            AND (
                ldo.org_created_at_ts IS NULL OR (
                    ldo.negate = FALSE AND (
                        CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(o.created_at) >= DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
                        WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(o.created_at) > DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
                        WHEN ldo.op IN ('before', 'lessThan') THEN DATE(o.created_at) < DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(o.created_at) >= DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
                        WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(o.created_at) > DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
                        WHEN ldo.op IN ('before', 'lessThan') THEN DATE(o.created_at) < DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
                    )
                )
            )
            AND (
                SIZE(ldo.org_locales) = 0 OR (
                    ldo.negate = FALSE AND (
                        ARRAY_CONTAINS(ldo.org_locales, o.locale)
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        ARRAY_CONTAINS(ldo.org_locales, o.locale)
                    )
                )
            )
            AND (
                SIZE(ldo.sam_numbers) = 0 OR (
                    ldo.negate = FALSE AND (
                        ARRAY_CONTAINS(ldo.sam_numbers, o.sam_number)
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        ARRAY_CONTAINS(ldo.sam_numbers, o.sam_number)
                    )
                )
            )
            AND (
                SIZE(ldo.org_release_types) = 0 OR (
                    ldo.negate = FALSE AND (
                        (ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER') AND o.release_type = 'Early Adopter') OR
                        (ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1') AND o.release_type = 'Phase 1') OR
                        (ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2') AND o.release_type = 'Phase 2')
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        (ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER') AND o.release_type = 'Early Adopter') OR
                        (ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1') AND o.release_type = 'Phase 1') OR
                        (ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2') AND o.release_type = 'Phase 2')
                    )
                )
            )
            AND (
                SIZE(ldo.is_internal) = 0 OR (
                    ldo.negate = FALSE AND (
                        ARRAY_CONTAINS(ldo.is_internal, 'true') AND c.org_category = 'Internal Orgs'
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        ARRAY_CONTAINS(ldo.is_internal, 'true') AND c.org_category = 'Internal Orgs'
                    )
                )
            )
        )
    LEFT OUTER JOIN pubsec_us p
        ON o.sam_number = p.sam_number
    WHERE
        o.date = '{PARTITION_START}'
        AND ldo.variation IS NULL -- Exclude

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
        o.locale,
        o.created_at,
        o.release_type,
        'eu-west-1' AS region
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
    JOIN data_tools_delta_share.product_analytics_staging.dim_organizations_classification c
        ON o.date = c.date
        AND o.org_id = c.org_id
    LEFT OUTER JOIN account_arr a
        ON o.account_id = a.account_id
    LEFT OUTER JOIN account_arr_sam a_sam
        ON o.sam_number = a_sam.sam_number
    LEFT OUTER JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
        ON (
            LOWER(ldo.variation) = 'false'
            AND (
                SIZE(ldo.org_ids) = 0
                OR (
                    ldo.negate = FALSE AND (
                        CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(o.org_id AS STRING))
                        WHEN ldo.op = 'lessThan' THEN o.org_id < CAST(ldo.org_ids[0] AS BIGINT)
                        WHEN ldo.op = 'greaterThan' THEN o.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(o.org_id AS STRING))
                        WHEN ldo.op = 'lessThan' THEN o.org_id < CAST(ldo.org_ids[0] AS BIGINT)
                        WHEN ldo.op = 'greaterThan' THEN o.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
                    )
                )
            )
            AND (
                ldo.org_created_at_ts IS NULL OR (
                    ldo.negate = FALSE AND (
                        CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(o.created_at) >= DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
                        WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(o.created_at) > DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
                        WHEN ldo.op IN ('before', 'lessThan') THEN DATE(o.created_at) < DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(o.created_at) >= DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
                        WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(o.created_at) > DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
                        WHEN ldo.op IN ('before', 'lessThan') THEN DATE(o.created_at) < DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
                    )
                )
            )
            AND (
                SIZE(ldo.org_locales) = 0 OR (
                    ldo.negate = FALSE AND (
                        ARRAY_CONTAINS(ldo.org_locales, o.locale)
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        ARRAY_CONTAINS(ldo.org_locales, o.locale)
                    )
                )
            )
            AND (
                SIZE(ldo.sam_numbers) = 0 OR (
                    ldo.negate = FALSE AND (
                        ARRAY_CONTAINS(ldo.sam_numbers, o.sam_number)
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        ARRAY_CONTAINS(ldo.sam_numbers, o.sam_number)
                    )
                )
            )
            AND (
                SIZE(ldo.org_release_types) = 0 OR (
                    ldo.negate = FALSE AND (
                        (ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER') AND o.release_type = 'Early Adopter') OR
                        (ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1') AND o.release_type = 'Phase 1') OR
                        (ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2') AND o.release_type = 'Phase 2')
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        (ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER') AND o.release_type = 'Early Adopter') OR
                        (ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1') AND o.release_type = 'Phase 1') OR
                        (ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2') AND o.release_type = 'Phase 2')
                    )
                )
            )
            AND (
                SIZE(ldo.is_internal) = 0 OR (
                    ldo.negate = FALSE AND (
                        ARRAY_CONTAINS(ldo.is_internal, 'true') AND c.org_category = 'Internal Orgs'
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        ARRAY_CONTAINS(ldo.is_internal, 'true') AND c.org_category = 'Internal Orgs'
                    )
                )
            )
        )
    LEFT OUTER JOIN pubsec_eu p
        ON o.sam_number = p.sam_number
    WHERE
        o.date = '{PARTITION_START}'
        AND ldo.variation IS NULL -- Exclude
),
relevant_licenses AS (
    -- Relevant licenses is LIC-VG-ASAT

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
            fo.product_sku = 'LIC-VG-ASAT'
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
            fo.product_sku = 'LIC-VG-ASAT'
        )
),
final_relevant_orgs AS (
    -- Final relevant orgs are:
    -- 1. Those with the license

    SELECT
        ro.org_id,
        ro.org_name,
        ro.account_arr_segment,
        ro.account_size_segment_name,
        ro.account_billing_country,
        ro.sam_number,
        ro.account_id,
        ro.account_name,
        ro.org_category,
        ro.region
    FROM relevant_orgs ro
    JOIN relevant_licenses rl
        ON ro.org_id = rl.org_id
),
events AS (
    -- Usage is defined as all devices that transmitted a message
    SELECT
        org_id,
        date AS event_date,
        object_id,
        time
    FROM kinesisstats_history.osdsatellitetransmitteddata
    WHERE
       date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
    GROUP BY ALL
    HAVING
        COUNT(value.proto_value.satellite_transmitted_data.original_payload) * 34 > 0

    UNION ALL

    SELECT
        org_id,
        date AS event_date,
        object_id,
        time
    FROM delta.`s3://samsara-eu-kinesisstats-delta-lake/table/deduplicated/osDSatelliteTransmittedData`
    WHERE
        date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
    GROUP BY ALL
    HAVING
        COUNT(value.proto_value.satellite_transmitted_data.original_payload) * 34 > 0
)
-- Definitions:
-- usage_weekly: Usage in the last 7 days (messages sent)
-- usage_monthly: Usage in the last 28 days (messages sent)
-- usage_prior_month: Usage in the 28 days prior to the last 28 days (messages sent)
-- usage_weekly_prior_month: Usage in the 7 days prior to the last 28 days (messages sent)
-- daily_active_user: Number of unique users using the feature on the day in question (not used here)
-- daily_enabled_users: Number of unique users who had access to the feature on the day in question
-- weekly_active_users: Number of unique users using the feature in the last 7 days (not used here)
-- weekly_enabled_users: Number of unique users who had access to the feature in the last 7 days
-- monthly_active_users: Number of unique users using the feature in the last 28 days (not used here)
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
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN CONCAT(e.org_id, '_', e.object_id, '_', e.time) END), 0) AS usage_weekly,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN CONCAT(e.org_id, '_', e.object_id, '_', e.time) END), 0) AS usage_monthly,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND DATE_SUB('{PARTITION_START}', 28) THEN CONCAT(e.org_id, '_', e.object_id, '_', e.time) END), 0) AS usage_prior_month,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 34) AND DATE_SUB('{PARTITION_START}', 28) THEN CONCAT(e.org_id, '_', e.object_id, '_', e.time) END), 0) AS usage_weekly_prior_month,
    CAST(0 AS BIGINT) AS daily_active_user,
    CAST(0 AS BIGINT) AS daily_enabled_users,
    CAST(0 AS BIGINT) AS weekly_active_users,
    CAST(0 AS BIGINT) AS weekly_enabled_users,
    CAST(0 AS BIGINT) AS monthly_active_users,
    CAST(0 AS BIGINT) AS monthly_enabled_users,
    MAX(CASE WHEN e.event_date = '{PARTITION_START}' THEN 1 ELSE 0 END) AS org_active_day,
    CASE WHEN usage_weekly > 0 THEN 1 ELSE 0 END AS org_active_week,
    CASE WHEN usage_weekly_prior_month > 0 THEN 1 ELSE 0 END AS org_active_week_prior_month,
    CASE WHEN usage_monthly > 0 THEN 1 ELSE 0 END AS org_active_month,
    CASE WHEN usage_prior_month > 0 THEN 1 ELSE 0 END AS org_active_prior_month
FROM final_relevant_orgs o
LEFT OUTER JOIN events e
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
    o.account_name
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""(Global) As part of Executive Scorecard, this shows usage stats for the Satellite Connectivity feature""",
        row_meaning="""Each row represents an org's usage of Satellite Connectivity over various time periods""",
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
        NonEmptyDQCheck(name="dq_non_empty_agg_executive_scorecard_satellite_connectivity_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_executive_scorecard_satellite_connectivity_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_executive_scorecard_satellite_connectivity_global",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_satellite_connectivity_global_usage",
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
            name="dq_sql_agg_executive_scorecard_satellite_connectivity_global_high_usage",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE usage_weekly > 10000
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        TrendDQCheck(name="dq_trend_agg_executive_scorecard_satellite_connectivity_global_enablement", lookback_days=1, tolerance=0.02),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_satellite_connectivity_global_usage_change",
            sql_query="""
                WITH today_usage AS (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS today_count
                    FROM df
                ),
                last_week_usage as (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS last_week_count
                    FROM product_analytics_staging.agg_executive_scorecard_satellite_connectivity_global
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
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1:product_analytics_staging.dim_organizations_classification",
        "us-west-2:datamodel_launchdarkly_bronze.segment_flag",
        "us-west-2:datamodel_launchdarkly_bronze.rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_variation",
        "us-west-2:release_management.ld_flag_variation_index_map",
        "us-west-2|eu-west-1:kinesisstats_history.osdsatellitetransmitteddata",
        "us-west-2|eu-west-1:finopsdb.customer_info",
    ],
)
def agg_executive_scorecard_satellite_connectivity_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_executive_scorecard_satellite_connectivity_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        flag_name="satellite-connectivity-license",
        INITIAL_LAUNCHDARKLY_DATE=INITIAL_LAUNCHDARKLY_DATE,
        ACCOUNT_BILLING_COUNTRY=ACCOUNT_BILLING_COUNTRY,
    )
    context.log.info(f"{query}")
    return query
