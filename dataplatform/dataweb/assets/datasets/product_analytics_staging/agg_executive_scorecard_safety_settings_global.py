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
)
from dataweb.userpkgs.constants import (
    ACCOUNT_BILLING_COUNTRY,
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id", "feature"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Week for which usage is being tracked",
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
        "name": "feature",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Safety setting being tracked",
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
            "comment": "Usage in the last 7 days",
        },
    },
    {
        "name": "usage_monthly",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Usage in the last 28 days",
        },
    },
    {
        "name": "usage_prior_month",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Usage in the 28 days prior to the last 28 days",
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
    -- No upfront org restriction, handled below based on licenses/devices
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
        'us-west-2' AS region,
        o.locale
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
        'eu-west-1' AS region,
        o.locale
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
    -- Look for the relevant CM licenses for safety events
    -- 1. PREMIER
    -- 2. ENTERPRISE
    -- 3. PREM
    -- 4. ENT
    -- 5. CM1-ENT/CM2-ENT
    -- 6. CM1-EXPRESS/CM2-EXPRESS (this is Essential, so need to handle a little differently)
    -- Anything with CM2 or CM-D is dual-facing, whereas CM1 or CM-S is single-facing
    SELECT DISTINCT o.org_id,
    fo.product_sku AS sku,
    xref.tier,
    CASE WHEN fo.product_sku LIKE 'LIC-CM2%' OR fo.product_sku LIKE 'LIC-CM-D%' THEN 'dual-facing CM' ELSE 'single-facing CM' END AS relevant_devices
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
            (
                fo.product_sku = 'LIC-CM-D-PREMIER'
                OR fo.product_sku = 'LIC-CM-D-ENTERPRISE'
                OR fo.product_sku = 'LIC-CM-S-PREMIER'
                OR fo.product_sku = 'LIC-CM-S-ENTERPRISE'
                OR fo.product_sku LIKE 'LIC-CM-D-PREM-PLTFM%'
                OR fo.product_sku LIKE 'LIC-CM-S-PREM-PLTFM%'
                OR fo.product_sku LIKE 'LIC-CM-D-ENT-PLTFM%'
                OR fo.product_sku LIKE 'LIC-CM-S-ENT-PLTFM%'
                OR fo.product_sku = 'LIC-CM1-ENT'
                OR fo.product_sku = 'LIC-CM2-ENT'
                OR fo.product_sku = 'LIC-CM1-EXPRESS'
                OR fo.product_sku = 'LIC-CM2-EXPRESS'
            )
            AND xref.is_core_license
        )

    UNION ALL

    SELECT DISTINCT o.org_id,
    fo.product_sku AS sku,
    xref.tier,
    CASE WHEN fo.product_sku LIKE 'LIC-CM2%' OR fo.product_sku LIKE 'LIC-CM-D%' THEN 'dual-facing CM' ELSE 'single-facing CM' END AS relevant_devices
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
            (
                fo.product_sku = 'LIC-CM-D-PREMIER'
                OR fo.product_sku = 'LIC-CM-D-ENTERPRISE'
                OR fo.product_sku = 'LIC-CM-S-PREMIER'
                OR fo.product_sku = 'LIC-CM-S-ENTERPRISE'
                OR fo.product_sku LIKE 'LIC-CM-D-PREM-PLTFM%'
                OR fo.product_sku LIKE 'LIC-CM-S-PREM-PLTFM%'
                OR fo.product_sku LIKE 'LIC-CM-D-ENT-PLTFM%'
                OR fo.product_sku LIKE 'LIC-CM-S-ENT-PLTFM%'
                OR fo.product_sku = 'LIC-CM1-ENT'
                OR fo.product_sku = 'LIC-CM2-ENT'
                OR fo.product_sku = 'LIC-CM1-EXPRESS'
                OR fo.product_sku = 'LIC-CM2-EXPRESS'
            )
            AND xref.is_core_license
        )
),
detection_types AS (
    -- The license type and devices applicable to a given detection type
    SELECT 'haTailgating' AS detection_type, 'premier_enterprise' AS allowed_tiers, 'single-facing CM' AS device_type -- Following Distance

    UNION ALL

    SELECT 'haPhonePolicy', 'premier_enterprise', 'dual-facing CM' -- Mobile Usage

    UNION ALL

    SELECT 'haDrowsinessDetection', 'premier_enterprise', 'dual-facing CM' -- Drowsiness Detection

    UNION ALL

    SELECT 'haNearCollision', 'premier_enterprise', 'single-facing CM' -- Forward Collision Warning

    UNION ALL

    SELECT 'haLaneDeparture', 'premier_enterprise', 'single-facing CM' --Lane Departure Warning

    UNION ALL

    SELECT 'haSeatbeltPolicy', 'premier_enterprise', 'dual-facing CM' -- No Seatbelt

    UNION ALL

    SELECT 'haDriverObstructionPolicy', 'premier_enterprise', 'dual-facing CM' -- Inward Obstruction

    UNION ALL

    SELECT 'haDistractedDriving', 'premier_enterprise', 'dual-facing CM' -- Inattentive Driving

    UNION ALL

    SELECT 'haTileRollingStopSign', 'premier_enterprise', 'single-facing CM' -- Rolling Stop

    UNION ALL

    SELECT 'haRolledStopSign', 'premier_enterprise', 'single-facing CM' -- Rolling Stop
),
feature_license_requirements AS (
    -- Relevant feature/device type mapping
    -- This helps filter orgs to what features they are enabled for

    SELECT 'Mobile Usage' AS feature, 'dual-facing CM' AS device_type

    UNION ALL

    SELECT 'Drowsiness Detection' AS feature, 'dual-facing CM' AS device_type

    UNION ALL

    SELECT 'Following Distance' AS feature, 'single-facing CM' AS device_type

    UNION ALL

    SELECT 'Forward Collision Warning' AS feature, 'single-facing CM' AS device_type

    UNION ALL

    SELECT 'Severe Speeding' AS feature, 'single-facing CM' AS device_type

    UNION ALL

    SELECT 'Lane Departure Warning' AS feature, 'single-facing CM' AS device_type

    UNION ALL

    SELECT 'No Seatbelt' AS feature, 'dual-facing CM' AS device_type

    UNION ALL

    SELECT 'Inward Obstruction' AS feature, 'dual-facing CM' AS device_type

    UNION ALL

    SELECT 'Inattentive Driving' AS feature, 'dual-facing CM' AS device_type

    UNION ALL

    SELECT 'Rolling Stop' AS feature, 'single-facing CM' AS device_type
),
lifetime_activity_combined_us AS (
    -- Pre-aggregate lifetime activity for both current and prior period in one scan
    -- This reduces 2 joins to lifetime_device_activity down to 1
    SELECT
        device_id,
        org_id,
        MAX(CASE WHEN date = '{PARTITION_START}' THEN l1 END) AS l1,
        MAX(CASE WHEN date = '{PARTITION_START}' THEN l7 END) AS l7,
        MAX(CASE WHEN date = '{PARTITION_START}' THEN l28 END) AS l28,
        MAX(CASE WHEN date = DATE_SUB('{PARTITION_START}', 28) THEN l7 END) AS p7,
        MAX(CASE WHEN date = DATE_SUB('{PARTITION_START}', 28) THEN l28 END) AS p28
    FROM datamodel_core.lifetime_device_activity
    WHERE date IN ('{PARTITION_START}', DATE_SUB('{PARTITION_START}', 28))
    GROUP BY device_id, org_id
),
lifetime_activity_combined_eu AS (
    -- Pre-aggregate lifetime activity for both current and prior period in one scan
    SELECT
        device_id,
        org_id,
        MAX(CASE WHEN date = '{PARTITION_START}' THEN l1 END) AS l1,
        MAX(CASE WHEN date = '{PARTITION_START}' THEN l7 END) AS l7,
        MAX(CASE WHEN date = '{PARTITION_START}' THEN l28 END) AS l28,
        MAX(CASE WHEN date = DATE_SUB('{PARTITION_START}', 28) THEN l7 END) AS p7,
        MAX(CASE WHEN date = DATE_SUB('{PARTITION_START}', 28) THEN l28 END) AS p28
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/lifetime_device_activity`
    WHERE date IN ('{PARTITION_START}', DATE_SUB('{PARTITION_START}', 28))
    GROUP BY device_id, org_id
),
daily_snapshot_source_us AS (
    -- Get Dojo data for last 56 days
    SELECT
        date,
        feature_enabled,
        org_id,
        vg_device_id,
        cm_device_id
    FROM dojo.device_ai_features_daily_snapshot
    WHERE
        date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        AND (ai_release_stage IN ('CLOSED_BETA', 'OPEN_BETA') OR ai_release_stage IS NULL)
        AND feature_enabled IN (
            'haDrowsinessDetection',
            'haNearCollision',
            'haTailgating',
            'haPhonePolicy',
            'haLaneDeparture',
            'haSeatbeltPolicy',
            'haTileRollingStopSign',
            'haRolledStopSign',
            'haDriverObstructionPolicy',
            'haDistractedDriving'
        )
),
daily_snapshot_source_eu AS (
    SELECT
        date,
        feature_enabled,
        org_id,
        vg_device_id,
        cm_device_id
    FROM data_tools_delta_share.dojo.device_ai_features_daily_snapshot
    WHERE
        date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        AND (ai_release_stage IN ('CLOSED_BETA', 'OPEN_BETA') OR ai_release_stage IS NULL)
        AND feature_enabled IN (
            'haDrowsinessDetection',
            'haNearCollision',
            'haTailgating',
            'haPhonePolicy',
            'haLaneDeparture',
            'haSeatbeltPolicy',
            'haTileRollingStopSign',
            'haRolledStopSign',
            'haDriverObstructionPolicy',
            'haDistractedDriving'
        )
),
daily_cms_base_data_for_joins AS (
    -- Get all device-related data for the last 56 days
    -- We filter to active devices and then look for VGs to be attached to a CM for the relevant features
    -- Mobile Usage/Drowsiness Detection/No Seatbelt/Inward Obstruction/Inattentive Driving: Dual-facing CMs
    -- Following Distance/Forward Collision Warning/Lane Departure Warning/Rolling Stop: Single-facing CMs
    SELECT /*+ BROADCAST(dt) */ DISTINCT
        dd.date,
        CASE WHEN
            dt.detection_type = 'haDrowsinessDetection'
                THEN 'Drowsiness Detection'
        WHEN
            dt.detection_type = 'haNearCollision'
                THEN 'Forward Collision Warning'
        WHEN
            dt.detection_type = 'haTailgating'
                THEN 'Following Distance'
        WHEN
            dt.detection_type = 'haPhonePolicy'
                THEN 'Mobile Usage'
        WHEN
            dt.detection_type = 'haLaneDeparture'
                THEN 'Lane Departure Warning'
        WHEN
            dt.detection_type = 'haSeatbeltPolicy'
                THEN 'No Seatbelt'
        WHEN
            dt.detection_type = 'haTileRollingStopSign'
                THEN 'Rolling Stop'
        WHEN
            dt.detection_type = 'haRolledStopSign'
                THEN 'Rolling Stop'
        WHEN
            dt.detection_type = 'haDriverObstructionPolicy'
                THEN 'Inward Obstruction'
        WHEN
            dt.detection_type = 'haDistractedDriving'
                THEN 'Inattentive Driving'
        END AS detection_type,
        dd.org_id,
        dd.device_id,
        l.l1,
        l.l7,
        l.l28,
        l.p7,
        l.p28,
        CASE WHEN d_snap.feature_enabled IS NOT NULL THEN 1 ELSE 0 END AS is_feature_enabled
    FROM datamodel_core.dim_devices dd
    JOIN lifetime_activity_combined_us l  -- Single join instead of two
        ON dd.device_id = l.device_id
        AND dd.org_id = l.org_id
    JOIN relevant_licenses lt
        ON dd.org_id = lt.org_id
    JOIN detection_types dt
        ON ( -- Eligibility based on license tier
            (dt.allowed_tiers = 'all' AND lt.tier IN ('Enterprise', 'Premier', 'Essential')) OR
            (dt.allowed_tiers = 'premier_enterprise' AND (lt.tier IN ('Enterprise', 'Premier') OR lt.sku LIKE '%EXPRESS')) OR
            (dt.allowed_tiers = 'enterprise' AND lt.tier = 'Enterprise')
        )
        AND ( -- Eligibility based on device product name for the detection type
            (dt.device_type = 'dual-facing CM' AND dd.product_name IN ('CM32', 'CM34') AND lt.relevant_devices = 'dual-facing CM') OR
            (dt.device_type = 'single-facing CM' AND dd.product_name IN ('CM31', 'CM32', 'CM33', 'CM34') AND lt.relevant_devices IN ('dual-facing CM', 'single-facing CM')) OR
            (dt.device_type = 'VG' AND dd.product_name IN ('VG34', 'VG54-NA', 'VG55-NA'))
        )
    LEFT JOIN daily_snapshot_source_us d_snap
        ON dd.date = d_snap.date
        AND dt.detection_type = d_snap.feature_enabled -- Matching the specific feature
        AND dd.org_id = d_snap.org_id -- Ensure join keys are same type
        AND (
            (dt.device_type IN ('dual-facing CM', 'single-facing CM') AND dd.device_id = d_snap.cm_device_id) OR
            (dt.device_type = 'VG' AND dd.device_id = d_snap.vg_device_id)
        )
    WHERE
        dd.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'

    UNION ALL

    SELECT /*+ BROADCAST(dt) */ DISTINCT
        dd.date,
        CASE WHEN
            dt.detection_type = 'haDrowsinessDetection'
                THEN 'Drowsiness Detection'
        WHEN
            dt.detection_type = 'haNearCollision'
                THEN 'Forward Collision Warning'
        WHEN
            dt.detection_type = 'haTailgating'
                THEN 'Following Distance'
        WHEN
            dt.detection_type = 'haPhonePolicy'
                THEN 'Mobile Usage'
        WHEN
            dt.detection_type = 'haLaneDeparture'
                THEN 'Lane Departure Warning'
        WHEN
            dt.detection_type = 'haSeatbeltPolicy'
                THEN 'No Seatbelt'
        WHEN
            dt.detection_type = 'haTileRollingStopSign'
                THEN 'Rolling Stop'
        WHEN
            dt.detection_type = 'haRolledStopSign'
                THEN 'Rolling Stop'
        WHEN
            dt.detection_type = 'haDriverObstructionPolicy'
                THEN 'Inward Obstruction'
        WHEN
            dt.detection_type = 'haDistractedDriving'
                THEN 'Inattentive Driving'
        END AS detection_type,
        dd.org_id,
        dd.device_id,
        l.l1,
        l.l7,
        l.l28,
        l.p7,
        l.p28,
        CASE WHEN d_snap.feature_enabled IS NOT NULL THEN 1 ELSE 0 END AS is_feature_enabled
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` dd
    JOIN lifetime_activity_combined_eu l  -- Single join instead of two
        ON dd.device_id = l.device_id
        AND dd.org_id = l.org_id
    JOIN relevant_licenses lt
        ON dd.org_id = lt.org_id
    JOIN detection_types dt
        ON ( -- Eligibility based on license tier
            (dt.allowed_tiers = 'all' AND lt.tier IN ('Enterprise', 'Premier', 'Essential')) OR
            (dt.allowed_tiers = 'premier_enterprise' AND (lt.tier IN ('Enterprise', 'Premier') OR lt.sku LIKE '%EXPRESS')) OR
            (dt.allowed_tiers = 'enterprise' AND lt.tier = 'Enterprise')
        )
        AND ( -- Eligibility based on device product name for the detection type
            (dt.device_type = 'dual-facing CM' AND dd.product_name IN ('CM32', 'CM34') AND lt.relevant_devices = 'dual-facing CM') OR
            (dt.device_type = 'single-facing CM' AND dd.product_name IN ('CM31', 'CM32', 'CM33', 'CM34') AND lt.relevant_devices IN ('dual-facing CM', 'single-facing CM')) OR
            (dt.device_type = 'VG' AND dd.product_name IN ('VG34', 'VG54-NA', 'VG55-NA'))
        )
    LEFT JOIN daily_snapshot_source_eu d_snap
        ON dd.date = d_snap.date
        AND dt.detection_type = d_snap.feature_enabled -- Matching the specific feature
        AND dd.org_id = d_snap.org_id -- Ensure join keys are same type
        AND (
            (dt.device_type IN ('dual-facing CM', 'single-facing CM') AND dd.device_id = d_snap.cm_device_id) OR
            (dt.device_type = 'VG' AND dd.device_id = d_snap.vg_device_id)
        )
    WHERE
        dd.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
),
settings_device_agg AS (
    -- If at least half the active devices in a given timespan have the feature enabled, usage would then be the number of active devices in that timeframe
    -- We can use l1, l7, l28 to reduce our reads on the lifetime table
    SELECT
        org_id,
        detection_type,
        COUNT(DISTINCT CASE WHEN l1 > 0 AND date = '{PARTITION_START}' THEN device_id END) AS eligible_l1,
        COUNT(DISTINCT CASE WHEN l1 > 0 AND date = '{PARTITION_START}' AND is_feature_enabled = 1 THEN device_id END) AS usage_l1,
        COUNT(DISTINCT CASE WHEN l7 > 0 AND date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN device_id END) AS eligible_l7,
        COUNT(DISTINCT CASE WHEN l7 > 0 AND date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' AND is_feature_enabled = 1 THEN device_id END) AS usage_l7,
        COUNT(DISTINCT CASE WHEN l28 > 0 AND date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN device_id END) AS eligible_l28,
        COUNT(DISTINCT CASE WHEN l28 > 0 AND date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' AND is_feature_enabled = 1 THEN device_id END) AS usage_l28,
        COUNT(DISTINCT CASE WHEN p28 > 0 AND date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND DATE_SUB('{PARTITION_START}', 28) THEN device_id END) AS eligible_p28,
        COUNT(DISTINCT CASE WHEN p28 > 0 AND date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND DATE_SUB('{PARTITION_START}', 28) AND is_feature_enabled = 1 THEN device_id END) AS usage_p28,
        COUNT(DISTINCT CASE WHEN p7 > 0 AND date BETWEEN DATE_SUB('{PARTITION_START}', 34) AND DATE_SUB('{PARTITION_START}', 28) THEN device_id END) AS eligible_p7,
        COUNT(DISTINCT CASE WHEN p7 > 0 AND date BETWEEN DATE_SUB('{PARTITION_START}', 34) AND DATE_SUB('{PARTITION_START}', 28) AND is_feature_enabled = 1 THEN device_id END) AS usage_p7
    FROM daily_cms_base_data_for_joins
    GROUP BY
        org_id,
        detection_type
),
settings_device_grouped AS (
    -- Overall enablement is based on licenses only
    -- Usage will be 0 when at least half the active devices do not have the feature enabled, but will be the number of active devices in the timespan otherwise
    SELECT
        org_id,
        detection_type,
        CASE WHEN eligible_l1 > 0 AND usage_l1 * 1.0 / eligible_l1 >= 0.5 THEN usage_l1 ELSE 0 END AS usage_l1,
        CASE WHEN eligible_l7 > 0 AND usage_l7 * 1.0 / eligible_l7 >= 0.5 THEN usage_l7 ELSE 0 END AS usage_l7,
        CASE WHEN eligible_l28 > 0 AND usage_l28 * 1.0 / eligible_l28 >= 0.5 THEN usage_l28 ELSE 0 END AS usage_l28,
        CASE WHEN eligible_p28 > 0 AND usage_p28 * 1.0 / eligible_p28 >= 0.5 THEN usage_p28 ELSE 0 END AS usage_p28,
        CASE WHEN eligible_p7 > 0 AND usage_p7 * 1.0 / eligible_p7 >= 0.5 THEN usage_p7 ELSE 0 END AS usage_p7
    FROM settings_device_agg
),
severe_speeding_settings AS (
    -- Overall enablement and active device numbers for severe speeding VGs
    -- Orgs must have the proper licenses to be considered enabled
    -- Usage will be the number of active VGs in that timeframe if at least half of the active VGs have severe speeding enabled
    SELECT
        d.org_id,
        'Severe Speeding' AS detection_type,
        COUNT(DISTINCT CASE WHEN l.l1 > 0 AND d.date = '{PARTITION_START}' THEN d.device_id END) AS eligible_l1,
        COUNT(DISTINCT CASE WHEN l.l1 > 0 AND d.date = '{PARTITION_START}' AND ss.severe_speeding_enabled THEN d.device_id END) AS usage_l1,
        COUNT(DISTINCT CASE WHEN l.l7 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN d.device_id END) AS eligible_l7,
        COUNT(DISTINCT CASE WHEN l.l7 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' AND ss.severe_speeding_enabled THEN d.device_id END) AS usage_l7,
        COUNT(DISTINCT CASE WHEN l.l28 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN d.device_id END) AS eligible_l28,
        COUNT(DISTINCT CASE WHEN l.l28 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' AND ss.severe_speeding_enabled THEN d.device_id END) AS usage_l28,
        COUNT(DISTINCT CASE WHEN l.p28 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND DATE_SUB('{PARTITION_START}', 28) THEN d.device_id END) AS eligible_p28,
        COUNT(DISTINCT CASE WHEN l.p28 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND DATE_SUB('{PARTITION_START}', 28) AND ss.severe_speeding_enabled THEN d.device_id END) AS usage_p28,
        COUNT(DISTINCT CASE WHEN l.p7 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 34) AND DATE_SUB('{PARTITION_START}', 28) THEN d.device_id END) AS eligible_p7,
        COUNT(DISTINCT CASE WHEN l.p7 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 34) AND DATE_SUB('{PARTITION_START}', 28) AND ss.severe_speeding_enabled THEN d.device_id END) AS usage_p7
    FROM datamodel_core.dim_devices d
    JOIN lifetime_activity_combined_us l  -- Single join instead of two
        ON l.org_id = d.org_id
        AND l.device_id = d.device_id
    LEFT JOIN product_analytics.dim_devices_safety_settings ss
        ON ss.org_id = d.org_id
        AND ss.vg_device_id = d.device_id
        AND ss.date = d.date
        AND ss.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
    WHERE
        d.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        AND d.device_type = 'VG - Vehicle Gateway'
    GROUP BY
        d.org_id

    UNION ALL

    SELECT
        d.org_id,
        'Severe Speeding' AS detection_type,
        COUNT(DISTINCT CASE WHEN l.l1 > 0 AND d.date = '{PARTITION_START}' THEN d.device_id END) AS eligible_l1,
        COUNT(DISTINCT CASE WHEN l.l1 > 0 AND d.date = '{PARTITION_START}' AND ss.severe_speeding_enabled THEN d.device_id END) AS usage_l1,
        COUNT(DISTINCT CASE WHEN l.l7 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' THEN d.device_id END) AS eligible_l7,
        COUNT(DISTINCT CASE WHEN l.l7 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 6) AND '{PARTITION_START}' AND ss.severe_speeding_enabled THEN d.device_id END) AS usage_l7,
        COUNT(DISTINCT CASE WHEN l.l28 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' THEN d.device_id END) AS eligible_l28,
        COUNT(DISTINCT CASE WHEN l.l28 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 27) AND '{PARTITION_START}' AND ss.severe_speeding_enabled THEN d.device_id END) AS usage_l28,
        COUNT(DISTINCT CASE WHEN l.p28 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND DATE_SUB('{PARTITION_START}', 28) THEN d.device_id END) AS eligible_p28,
        COUNT(DISTINCT CASE WHEN l.p28 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND DATE_SUB('{PARTITION_START}', 28) AND ss.severe_speeding_enabled THEN d.device_id END) AS usage_p28,
        COUNT(DISTINCT CASE WHEN l.p7 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 34) AND DATE_SUB('{PARTITION_START}', 28) THEN d.device_id END) AS eligible_p7,
        COUNT(DISTINCT CASE WHEN l.p7 > 0 AND d.date BETWEEN DATE_SUB('{PARTITION_START}', 34) AND DATE_SUB('{PARTITION_START}', 28) AND ss.severe_speeding_enabled THEN d.device_id END) AS usage_p7
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` d
    JOIN lifetime_activity_combined_eu l  -- Single join instead of two
        ON l.org_id = d.org_id
        AND l.device_id = d.device_id
    LEFT JOIN data_tools_delta_share.product_analytics.dim_devices_safety_settings ss
        ON ss.org_id = d.org_id
        AND ss.vg_device_id = d.device_id
        AND ss.date = d.date
        AND ss.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
    WHERE
        d.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
        AND d.device_type = 'VG - Vehicle Gateway'
    GROUP BY
        d.org_id
),
severe_speeding_settings_grouped AS (
    -- Only show usage when feature is enabled by at least half of the active VGs
    SELECT
        org_id,
        detection_type,
        CASE WHEN eligible_l1 > 0 AND usage_l1 * 1.0 / eligible_l1 >= 0.5 THEN usage_l1 ELSE 0 END AS usage_l1,
        CASE WHEN eligible_l7 > 0 AND usage_l7 * 1.0 / eligible_l7 >= 0.5 THEN usage_l7 ELSE 0 END AS usage_l7,
        CASE WHEN eligible_l28 > 0 AND usage_l28 * 1.0 / eligible_l28 >= 0.5 THEN usage_l28 ELSE 0 END AS usage_l28,
        CASE WHEN eligible_p28 > 0 AND usage_p28 * 1.0 / eligible_p28 >= 0.5 THEN usage_p28 ELSE 0 END AS usage_p28,
        CASE WHEN eligible_p7 > 0 AND usage_p7 * 1.0 / eligible_p7 >= 0.5 THEN usage_p7 ELSE 0 END AS usage_p7
    FROM severe_speeding_settings
),
license_matches AS (
    -- Get orgs who can use each specific feature
    SELECT DISTINCT
        o.org_id,
        o.org_name,
        o.account_arr_segment,
        o.account_size_segment_name,
        o.account_billing_country,
        o.sam_number,
        o.account_id,
        o.account_name,
        o.org_category,
        o.region,
        f.feature
    FROM relevant_orgs o
    JOIN relevant_licenses rl
        ON rl.org_id = o.org_id
    JOIN feature_license_requirements f
        ON (
            f.device_type = 'dual-facing CM' AND rl.relevant_devices = 'dual-facing CM'
            OR f.device_type = 'single-facing CM' AND rl.relevant_devices IN ('single-facing CM', 'dual-facing CM')
        )
    WHERE CASE WHEN f.feature = 'Lane Departure Warning' THEN o.locale NOT IN ('at', 'be', 'ch', 'cz', 'de', 'dk', 'es', 'fr', 'gb', 'ie', 'im', 'it', 'lu', 'nl', 'pl', 'pt', 'ro', 'sk') -- Lane Departure Warning is not available for EU locales
    ELSE 1=1 END
),
final_data AS (
    -- Final join with events
    SELECT
        lm.*,
        COALESCE(s.detection_type, lm.feature) AS detection_type,
        s.usage_l1,
        s.usage_l7,
        s.usage_l28,
        s.usage_p28,
        s.usage_p7
    FROM license_matches lm
    LEFT OUTER JOIN settings_device_grouped s
        ON s.org_id = lm.org_id
        AND s.detection_type = lm.feature
    WHERE lm.feature != 'Severe Speeding'

    UNION ALL

    SELECT
        lm.*,
        COALESCE(s.detection_type, lm.feature) AS detection_type,
        s.usage_l1,
        s.usage_l7,
        s.usage_l28,
        s.usage_p28,
        s.usage_p7
    FROM license_matches lm
    LEFT OUTER JOIN severe_speeding_settings_grouped s
        ON s.org_id = lm.org_id
        AND s.detection_type = lm.feature
    WHERE lm.feature = 'Severe Speeding'
)
-- Definitions:
-- usage_weekly: Usage in the last 7 days (active devices)
-- usage_monthly: Usage in the last 28 days (active devices)
-- usage_prior_month: Usage in the 28 days prior to the last 28 days (active devices)
-- usage_weekly_prior_month: Usage in the 7 days prior to the last 28 days (active devices)
-- org_active_day: Whether the org used the feature on the day in question (met the criteria)
-- org_active_week: Whether the org used the feature in the last 7 days (met the criteria)
-- org_active_week_prior_month: Whether the org used the feature in the 7 days prior to the last 28 days (met the criteria)
-- org_active_month: Whether the org used the feature in the last 28 days (met the criteria)
-- org_active_prior_month: Whether the org used the feature in the 28 days prior to the last 28 days (met the criteria)
SELECT DISTINCT
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
    o.detection_type AS feature,
    1 AS enabled,
    COALESCE(usage_l7, 0) AS usage_weekly,
    COALESCE(usage_l28, 0) AS usage_monthly,
    COALESCE(usage_p28, 0) AS usage_prior_month,
    COALESCE(usage_p7, 0) AS usage_weekly_prior_month,
    CASE WHEN COALESCE(usage_l1, 0) > 0 THEN 1 ELSE 0 END AS org_active_day,
    CASE WHEN COALESCE(usage_l7, 0) > 0 THEN 1 ELSE 0 END AS org_active_week,
    CASE WHEN COALESCE(usage_p7, 0) > 0 THEN 1 ELSE 0 END AS org_active_week_prior_month,
    CASE WHEN COALESCE(usage_l28, 0) > 0 THEN 1 ELSE 0 END AS org_active_month,
    CASE WHEN COALESCE(usage_p28, 0) > 0 THEN 1 ELSE 0 END AS org_active_prior_month
FROM final_data o
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""(Global) As part of Executive Scorecard, this shows usage stats for safety settings feature""",
        row_meaning="""Each row represents an org's usage of safety settings over various time periods""",
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
        NonEmptyDQCheck(name="dq_non_empty_agg_executive_scorecard_safety_settings_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_executive_scorecard_safety_settings_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_executive_scorecard_safety_settings_global",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        TrendDQCheck(name="dq_trend_agg_executive_scorecard_safety_settings_global_enablement", lookback_days=1, tolerance=0.02),
        SQLDQCheck(
            name="dq_sql_agg_executive_scorecard_safety_settings_global_usage_change",
            sql_query="""
                WITH today_usage AS (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS today_count
                    FROM df
                ),
                last_week_usage as (
                    SELECT COUNT(DISTINCT CASE WHEN org_active_week = 1 THEN org_id END) AS last_week_count
                    FROM product_analytics_staging.agg_executive_scorecard_safety_settings_global
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
        "us-west-2|eu-west-1:product_analytics.dim_devices_safety_settings",
        "us-west-2|eu-west-1:dojo.device_ai_features_daily_snapshot",
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1:product_analytics_staging.dim_organizations_classification",
        "us-west-2|eu-west-1:datamodel_core.dim_devices",
        "us-west-2|eu-west-1:datamodel_core.lifetime_device_activity",
        "us-west-2|eu-west-1:finopsdb.customer_info",
    ],
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=8,
        spark_conf_overrides={
            "spark.delta.sharing.driver.accessThresholdToExpireMs": "7200000",
        }
    ),
)
def agg_executive_scorecard_safety_settings_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_executive_scorecard_safety_settings_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        ACCOUNT_BILLING_COUNTRY=ACCOUNT_BILLING_COUNTRY,
    )
    context.log.info(f"{query}")
    return query
