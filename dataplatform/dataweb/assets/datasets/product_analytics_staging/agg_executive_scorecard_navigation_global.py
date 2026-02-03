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
        o.created_at,
        o.locale,
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
        o.created_at,
        o.locale,
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
org_id_list AS (
    -- Get orgs from FF
    SELECT ro.*
    FROM relevant_orgs ro
    JOIN pivoted_conditions_exploded ldo -- Joining to see what specifically is enabled
        ON (
            LOWER(ldo.variation) = 'true'
            AND (
                SIZE(ldo.org_ids) = 0
                OR (
                    ldo.negate = FALSE AND (
                        CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(ro.org_id AS STRING))
                        WHEN ldo.op = 'lessThan' THEN ro.org_id < CAST(ldo.org_ids[0] AS BIGINT)
                        WHEN ldo.op = 'greaterThan' THEN ro.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        CASE WHEN ldo.op = 'in' THEN ARRAY_CONTAINS(ldo.org_ids, CAST(ro.org_id AS STRING))
                        WHEN ldo.op = 'lessThan' THEN ro.org_id < CAST(ldo.org_ids[0] AS BIGINT)
                        WHEN ldo.op = 'greaterThan' THEN ro.org_id > CAST(ldo.org_ids[0] AS BIGINT) END
                    )
                )
            )
            AND (
                ldo.org_created_at_ts IS NULL OR (
                    ldo.negate = FALSE AND (
                        CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(ro.created_at) >= DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
                        WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(ro.created_at) > DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
                        WHEN ldo.op IN ('before', 'lessThan') THEN DATE(ro.created_at) < DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        CASE WHEN ldo.op IN ('greaterThanOrEqual') THEN DATE(ro.created_at) >= DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
                        WHEN ldo.op IN ('after', 'greaterThan') THEN DATE(ro.created_at) > DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000))
                        WHEN ldo.op IN ('before', 'lessThan') THEN DATE(ro.created_at) < DATE(FROM_UNIXTIME(CAST(ldo.org_created_at_ts AS BIGINT) / 1000)) END
                    )
                )
            )
            AND (
                SIZE(ldo.org_locales) = 0 OR (
                    ldo.negate = FALSE AND (
                        ARRAY_CONTAINS(ldo.org_locales, ro.locale)
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        ARRAY_CONTAINS(ldo.org_locales, ro.locale)
                    )
                )
            )
            AND (
                SIZE(ldo.sam_numbers) = 0 OR (
                    ldo.negate = FALSE AND (
                        ARRAY_CONTAINS(ldo.sam_numbers, ro.sam_number)
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        ARRAY_CONTAINS(ldo.sam_numbers, ro.sam_number)
                    )
                )
            )
            AND (
                SIZE(ldo.org_release_types) = 0 OR (
                    ldo.negate = FALSE AND (
                        (ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER') AND ro.release_type = 'Early Adopter') OR
                        (ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1') AND ro.release_type = 'Phase 1') OR
                        (ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2') AND ro.release_type = 'Phase 2')
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        (ARRAY_CONTAINS(ldo.org_release_types, 'EARLY_ADOPTER') AND ro.release_type = 'Early Adopter') OR
                        (ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_1') AND ro.release_type = 'Phase 1') OR
                        (ARRAY_CONTAINS(ldo.org_release_types, 'PHASE_2') AND ro.release_type = 'Phase 2')
                    )
                )
            )
            AND (
                SIZE(ldo.is_internal) = 0 OR (
                    ldo.negate = FALSE AND (
                        ARRAY_CONTAINS(ldo.is_internal, 'true') AND ro.org_category = 'Internal Orgs'
                    )
                )
                OR (
                    ldo.negate = TRUE AND NOT (
                        ARRAY_CONTAINS(ldo.is_internal, 'true') AND ro.org_category = 'Internal Orgs'
                    )
                )
            )
        )
),
relevant_licenses AS (
    -- Relevant licenses are LIC-COMM-NAV/LIC-COMM-NAV-PROMO/LIC-FL-APPS
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
            fo.product_sku = 'LIC-COMM-NAV'
            OR fo.product_sku = 'LIC-COMM-NAV-PROMO'
            OR fo.product_sku = 'LIC-FL-APPS'
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
            fo.product_sku = 'LIC-COMM-NAV'
            OR fo.product_sku = 'LIC-COMM-NAV-PROMO'
            OR fo.product_sku = 'LIC-FL-APPS'
        )
),
relevant_licenses_deduped AS (
    SELECT DISTINCT org_id
    FROM relevant_licenses
),
final_relevant_orgs AS (
    -- Final set of relevant orgs for enablement
    -- 1. Orgs with relevant licenses
    -- 2. Orgs who are in the beta
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
    JOIN relevant_licenses_deduped rl
        ON ro.org_id = rl.org_id

    UNION

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
    FROM org_id_list ro
    JOIN relevant_licenses_deduped rl
        ON ro.org_id = rl.org_id
),
events_us AS (
    -- Usage is defined as orgs with at least one started navigation session
    SELECT
        org_id,
        date AS event_date,
        driver_id AS user_id,
        nav_session_uuid
    FROM datastreams_history.mobile_nav_routing_events
    WHERE
        org_id IS NOT NULL
        AND event_type = 'NavState-startingNavigation'
        AND date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        AND driver_id != 0
        AND driver_id IS NOT NULL
),
events_eu AS (
    -- Usage is defined as orgs with at least one started navigation session
    SELECT
        org_id,
        date AS event_date,
        driver_id AS user_id,
        nav_session_uuid
    FROM delta.`s3://samsara-eu-data-streams-delta-lake/mobile_nav_routing_events`
    WHERE
        org_id IS NOT NULL
        AND event_type = 'NavState-startingNavigation'
        AND date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        AND driver_id != 0
        AND driver_id IS NOT NULL
),
events AS (
    SELECT * FROM events_us

    UNION ALL

    SELECT * FROM events_eu
),
enabled_drivers AS (
    -- We calculate drivers separately as it's more efficient
    -- This only applies for orgs that have LIC-FL-APPS
    SELECT /*+ BROADCAST(rl) */
      dd.org_id AS org_id,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= '{PARTITION_START}' THEN dd.driver_id END) AS daily_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 6) THEN dd.driver_id END) AS weekly_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27) THEN dd.driver_id END) AS monthly_enabled_drivers
    FROM datamodel_platform.dim_drivers dd
    JOIN final_relevant_orgs rl
        ON dd.org_id = rl.org_id
    JOIN relevant_licenses rln
        ON dd.org_id = rln.org_id
        AND rln.sku = 'LIC-FL-APPS'
    WHERE
        dd.date = '{PARTITION_START}'
        AND (DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27))
    GROUP BY dd.org_id

    UNION ALL

    SELECT /*+ BROADCAST(rl) */
      dd.org_id AS org_id,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= '{PARTITION_START}' THEN dd.driver_id END) AS daily_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 6) THEN dd.driver_id END) AS weekly_enabled_drivers,
      COUNT(CASE WHEN DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27) THEN dd.driver_id END) AS monthly_enabled_drivers
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_drivers` dd
    JOIN final_relevant_orgs rl
        ON dd.org_id = rl.org_id
    JOIN relevant_licenses rln
        ON dd.org_id = rln.org_id
        AND rln.sku = 'LIC-FL-APPS'
    WHERE
        dd.date = '{PARTITION_START}'
        AND (DATE(dd.deleted_at) IN ('0101-01-01', '0101-01-02') OR DATE(dd.deleted_at) >= DATE_SUB('{PARTITION_START}', 27))
    GROUP BY dd.org_id
),
enabled_users_us AS (
    -- Enabled users are just drivers with the LIC-COMM-NAV license
    -- For orgs who have access without LIC-COMM-NAV, all drivers are considered to have access
    SELECT
      la.org_id AS org_id,
      COUNT(DISTINCT CASE WHEN DATE(la.end_time) IS NULL OR DATE(la.end_time) >= '{PARTITION_START}' THEN la.entity_id END) AS daily_enabled_drivers,
      COUNT(DISTINCT CASE WHEN DATE(la.end_time) IS NULL OR DATE(la.end_time) >= DATE_SUB('{PARTITION_START}', 6) THEN la.entity_id END) AS weekly_enabled_drivers,
      COUNT(DISTINCT CASE WHEN DATE(la.end_time) IS NULL OR DATE(la.end_time) >= DATE_SUB('{PARTITION_START}', 27) THEN la.entity_id END) AS monthly_enabled_drivers
    FROM datamodel_platform.dim_license_assignment la
    LEFT OUTER JOIN relevant_licenses rl
        ON la.org_id = rl.org_id
        AND rl.sku = 'LIC-FL-APPS'
    WHERE
        la.date = '{PARTITION_START}'
        AND la.sku IN ('LIC-COMM-NAV', 'LIC-COMM-NAV-PROMO')
        AND rl.org_id IS NULL -- Exclude orgs who have access via LIC-FL-APPS
    GROUP BY la.org_id
),
enabled_users_eu AS (
    -- Enabled users are just drivers with the LIC-COMM-NAV license
    SELECT
      la.org_id AS org_id,
      COUNT(DISTINCT CASE WHEN DATE(la.end_time) IS NULL OR DATE(la.end_time) >= '{PARTITION_START}' THEN la.entity_id END) AS daily_enabled_drivers,
      COUNT(DISTINCT CASE WHEN DATE(la.end_time) IS NULL OR DATE(la.end_time) >= DATE_SUB('{PARTITION_START}', 6) THEN la.entity_id END) AS weekly_enabled_drivers,
      COUNT(DISTINCT CASE WHEN DATE(la.end_time) IS NULL OR DATE(la.end_time) >= DATE_SUB('{PARTITION_START}', 27) THEN la.entity_id END) AS monthly_enabled_drivers
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_license_assignment` la
    LEFT OUTER JOIN relevant_licenses rl
        ON la.org_id = rl.org_id
        AND rl.sku = 'LIC-FL-APPS'
    WHERE
        la.date = '{PARTITION_START}'
        AND la.sku IN ('LIC-COMM-NAV', 'LIC-COMM-NAV-PROMO')
        AND rl.org_id IS NULL -- Exclude orgs who have access via LIC-FL-APPS
    GROUP BY la.org_id
),
enabled_users AS (
    SELECT * FROM enabled_users_us

    UNION ALL

    SELECT * FROM enabled_users_eu
)
-- Definitions:
-- usage_weekly: Usage in the last 7 days (navigation sessions)
-- usage_monthly: Usage in the last 28 days (navigation sessions)
-- usage_prior_month: Usage in the 28 days prior to the last 28 days (navigation sessions)
-- usage_weekly_prior_month: Usage in the 7 days prior to the last 28 days (navigation sessions)
-- daily_active_user: Number of unique users using the feature on the day in question (drivers)
-- daily_enabled_users: Number of unique users who had access to the feature on the day in question (drivers)
-- weekly_active_users: Number of unique users using the feature in the last 7 days (drivers)
-- weekly_enabled_users: Number of unique users who had access to the feature in the last 7 days (drivers)
-- monthly_active_users: Number of unique users using the feature in the last 28 days (drivers)
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
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN nav_session_uuid END), 0) AS usage_weekly,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN nav_session_uuid END), 0) AS usage_monthly,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND DATE_SUB('{PARTITION_START}', 28) THEN nav_session_uuid END), 0) AS usage_prior_month,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 34) AND DATE_SUB('{PARTITION_START}', 28) THEN nav_session_uuid END), 0) AS usage_weekly_prior_month,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date = '{PARTITION_START}' THEN e.user_id END), 0) AS daily_active_user,
    MAX(COALESCE(eu.daily_enabled_drivers, 0)) + MAX(COALESCE(ed.daily_enabled_drivers, 0)) AS daily_enabled_users,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN e.user_id END), 0) AS weekly_active_users,
    MAX(COALESCE(eu.weekly_enabled_drivers, 0)) + MAX(COALESCE(ed.weekly_enabled_drivers, 0)) AS weekly_enabled_users,
    COALESCE(COUNT(DISTINCT CASE WHEN e.event_date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN e.user_id END), 0) AS monthly_active_users,
    MAX(COALESCE(eu.monthly_enabled_drivers, 0)) + MAX(COALESCE(ed.monthly_enabled_drivers, 0)) AS monthly_enabled_users,
    MAX(CASE WHEN e.event_date = '{PARTITION_START}' THEN 1 ELSE 0 END) AS org_active_day,
    CASE WHEN usage_weekly > 0 THEN 1 ELSE 0 END AS org_active_week,
    CASE WHEN usage_weekly_prior_month > 0 THEN 1 ELSE 0 END AS org_active_week_prior_month,
    CASE WHEN usage_monthly > 0 THEN 1 ELSE 0 END AS org_active_month,
    CASE WHEN usage_prior_month > 0 THEN 1 ELSE 0 END AS org_active_prior_month
FROM final_relevant_orgs o
LEFT OUTER JOIN events e
    ON o.org_id = e.org_id
LEFT OUTER JOIN enabled_users eu
    ON o.org_id = eu.org_id
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
    o.account_name
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""(Global) As part of Executive Scorecard, this shows usage stats for the Navigation feature""",
        row_meaning="""Each row represents an org's usage of Navigation over various time periods""",
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
        NonEmptyDQCheck(name="dq_non_empty_agg_executive_scorecard_navigation_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_executive_scorecard_navigation_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_executive_scorecard_navigation_global",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_navigation_global_enabled_users",
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
            name="dq_sql_agg_executive_scorecard_navigation_global_usage",
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
            name="dq_sql_agg_executive_scorecard_navigation_global_high_usage",
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
            name="dq_sql_agg_executive_scorecard_navigation_global_high_active_users",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE weekly_active_users > 10000
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        TrendDQCheck(name="dq_trend_agg_executive_scorecard_navigation_global_enablement", lookback_days=1, tolerance=0.1),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_navigation_global_usage_change",
            sql_query="""
                WITH today_usage AS (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS today_count
                    FROM df
                ),
                last_week_usage as (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS last_week_count
                    FROM product_analytics_staging.agg_executive_scorecard_navigation_global
                    WHERE date = (SELECT DISTINCT DATE_SUB(date, 7) FROM df)
                )
                SELECT CASE WHEN last_week_count = 0 THEN 0
                            ELSE ABS(today_count - last_week_count) * 1.0 / last_week_count
                            END AS observed_value
                FROM today_usage
                CROSS JOIN last_week_usage
            """,
            expected_value=0.5,
            operator=Operator.lte,
            block_before_write=False,
        ),
    ],
    backfill_batch_size=1,
    upstreams=[
        "us-west-2:product_analytics_staging.stg_customer_enriched",
        "us-west-2|eu-west-1:datastreams_history.mobile_nav_routing_events",
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1:datamodel_platform.dim_drivers",
        "us-west-2|eu-west-1:product_analytics_staging.dim_organizations_classification",
        "us-west-2|eu-west-1:datamodel_platform.dim_license_assignment",
        "us-west-2:datamodel_launchdarkly_bronze.segment_flag",
        "us-west-2:datamodel_launchdarkly_bronze.rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_variation",
        "us-west-2:release_management.ld_flag_variation_index_map",
        "us-west-2|eu-west-1:finopsdb.customer_info",
    ],
)
def agg_executive_scorecard_navigation_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_executive_scorecard_navigation_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        flag_name='commercial-nav-here-enabled',
        INITIAL_LAUNCHDARKLY_DATE=INITIAL_LAUNCHDARKLY_DATE,
        ACCOUNT_BILLING_COUNTRY=ACCOUNT_BILLING_COUNTRY,
    )
    context.log.info(f"{query}")
    return query
