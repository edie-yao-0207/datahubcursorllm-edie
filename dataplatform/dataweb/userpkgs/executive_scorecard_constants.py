from dagster import DailyPartitionsDefinition
from dataweb.userpkgs.constants import ColumnDescription


PARTITIONING = DailyPartitionsDefinition(start_date="2024-08-01")
INITIAL_LAUNCHDARKLY_DATE = "2025-10-01"
MAX_RETRIES = 3

PRIMARY_KEYS = ["date", "org_id"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Day for which usage is being tracked",
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.ORG_ID,
        },
    },
    {
        "name": "org_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of the organization"
        },
    },
    {
        "name": "org_category",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category of org, based on licenses and internal users in corresponding account"
        },
    },
    {
        "name": "account_arr_segment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": """ARR from the previously publicly reporting quarter in the buckets:
                <0
                0 - 100k
                100k - 500K,
                500K - 1M
                1M+
                Note - ARR information is calculated at the account level, not the SAM Number level
                An account and have multiple SAMs. Additionally a SAM can have multiple accounts, resulting in a single SAM having multiple account_arr_segments
                """
        },
    },
    {
        "name": "account_size_segment_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "account_billing_country",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Classifies accounts by billing country"
        },
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Cloud region of the organization",
        },
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.SAM_NUMBER
        },
    },
    {
        "name": "account_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unique ID of the account"
        },
    },
    {
        "name": "account_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of the account"
        },
    },
    {
        "name": "enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org has access to the feature or not",
        },
    },
    {
        "name": "usage_weekly",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Feature usage in the last 7 days",
        },
    },
    {
        "name": "usage_monthly",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Feature usage in the last 28 days",
        },
    },
    {
        "name": "usage_prior_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Feature usage in the 28 days prior to the last 28 days",
        },
    },
    {
        "name": "usage_weekly_prior_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Feature usage in the 7 days prior to the last 28 days",
        },
    },
    {
        "name": "daily_active_user",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of active users for the day for the feature",
        },
    },
    {
        "name": "daily_enabled_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of distinct users who had access to use the feature on the day",
        },
    },
    {
        "name": "weekly_active_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of distinct users over last 7 days for the feature",
        },
    },
    {
        "name": "weekly_enabled_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of distinct users who had access to use the feature in the last 7 days",
        },
    },
    {
        "name": "monthly_active_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of distinct users over last 28 days for the feature",
        },
    },
    {
        "name": "monthly_enabled_users",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of distinct users who had access to use the feature in the last 28 days",
        },
    },
    {
        "name": "org_active_day",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org was active for the feature on the day or not",
        },
    },
    {
        "name": "org_active_week",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org was active for the feature in the last week or not",
        },
    },
    {
        "name": "org_active_week_prior_month",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org was active for the feature in the week 28 days ago or not",
        },
    },
    {
        "name": "org_active_month",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org was active for the feature in the last 28 days or not",
        },
    },
    {
        "name": "org_active_prior_month",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org was active for the feature in 28 days prior to the last 28 days or not",
        },
    },
]

LAUNCHDARKLY_FALSE_JOIN_CONDITION = """
(
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
                ARRAY_CONTAINS(ldo.is_internal, 'true') AND o.org_category = 'Internal Orgs'
            )
        )
        OR (
            ldo.negate = TRUE AND NOT (
                ARRAY_CONTAINS(ldo.is_internal, 'true') AND o.org_category = 'Internal Orgs'
            )
        )
    )
)
"""

LAUNCHDARKLY_TRUE_JOIN_CONDITION = """
(
    LOWER(ldo.variation) = 'true'
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
                ARRAY_CONTAINS(ldo.is_internal, 'true') AND o.org_category = 'Internal Orgs'
            )
        )
        OR (
            ldo.negate = TRUE AND NOT (
                ARRAY_CONTAINS(ldo.is_internal, 'true') AND o.org_category = 'Internal Orgs'
            )
        )
    )
)
"""

INTERNAL_USERS_CTE = """
users_us AS (
    -- Filter out internal/canary users
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
    -- Filter out internal/canary users
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
"""

SERVES_ALL_LICENSE_CTE = """
license_filter AS (
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
"""
