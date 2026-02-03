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
    load_product_usage_config,
)


QUERY = """
WITH {RELEVANT_ORGS}
total_active_brigid_us AS (
    SELECT DISTINCT
        dd.org_id,
        dd.device_id,
        lda.l1,
        lda.l7,
        lda.l28,
        COALESCE(pda.l7, 0) AS p7,
        COALESCE(pda.l28, 0) AS p28
    FROM datamodel_core.lifetime_device_activity lda
    JOIN datamodel_core.dim_devices dd
        ON lda.device_id = dd.device_id
        AND lda.org_id = dd.org_id
        AND lda.date = dd.date
    LEFT OUTER JOIN datamodel_core.lifetime_device_activity pda
        ON lda.device_id = pda.device_id
        AND lda.org_id = pda.org_id
        AND pda.date = DATE_SUB('{PARTITION_START}', 28)
    WHERE
        lda.date = '{PARTITION_START}'
        AND dd.product_id IN (155, 167) -- Brigid
),
total_active_brigid_eu AS (
    SELECT DISTINCT
        dd.org_id,
        dd.device_id,
        lda.l1,
        lda.l7,
        lda.l28,
        COALESCE(pda.l7, 0) AS p7,
        COALESCE(pda.l28, 0) AS p28
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/lifetime_device_activity` lda
    JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` dd
        ON lda.device_id = dd.device_id
        AND lda.org_id = dd.org_id
        AND lda.date = dd.date
    LEFT OUTER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/lifetime_device_activity` pda
        ON lda.device_id = pda.device_id
        AND lda.org_id = pda.org_id
        AND pda.date = DATE_SUB('{PARTITION_START}', 28)
    WHERE
        lda.date = '{PARTITION_START}'
        AND dd.product_id IN (155, 167) -- Brigid
),
all_roadside_settings_org_us AS (
    SELECT
        EntityId,
        Namespace,
        FROM_JSON(Settings, 'struct<vulnerableRoadUserCollisionWarningSettings:struct<audioAlerts:struct<enabled:boolean>, enabled:boolean, enabledCameraStreams:struct<front:boolean, left:boolean, rear:boolean, right:boolean, roadFacingDashcam:boolean>>>').vulnerableRoadUserCollisionWarningSettings.audioAlerts.enabled AS enabled,
        _approximate_creation_date_time
    FROM dynamodb.settings
    WHERE
        Namespace LIKE 'SafetyEventDetection%'
        AND EntityId LIKE 'ORG#%'
        AND DATE(FROM_UNIXTIME(_approximate_creation_date_time / 1000)) <= '{PARTITION_START}'
),
all_roadside_settings_device_us AS (
    SELECT
        EntityId,
        Namespace,
        FROM_JSON(Settings, 'struct<vulnerableRoadUserCollisionWarningSettings:struct<audioAlerts:struct<enabled:boolean>, enabled:boolean, enabledCameraStreams:struct<front:boolean, left:boolean, rear:boolean, right:boolean, roadFacingDashcam:boolean>>>').vulnerableRoadUserCollisionWarningSettings.audioAlerts.enabled AS enabled,
        _approximate_creation_date_time
    FROM dynamodb.settings
    WHERE
        Namespace LIKE 'SafetyEventDetection%'
        AND EntityId LIKE 'DEVICE#%'
        AND DATE(FROM_UNIXTIME(_approximate_creation_date_time / 1000)) <= '{PARTITION_START}'
),
latest_org_settings_us AS (
    -- Get the latest org-level settings per org
    SELECT
        REGEXP_EXTRACT(EntityId, 'ORG#(.+)', 1) AS org_id,
        enabled AS org_enabled,
        _approximate_creation_date_time AS org_timestamp
    FROM all_roadside_settings_org_us
    WHERE Namespace = 'SafetyEventDetection'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY EntityId ORDER BY _approximate_creation_date_time DESC) = 1
),
latest_device_settings_us AS (
    -- Get the latest device-level settings per device
    -- Settings stores VG device ID, so getting Brigid device ID from dim_devices
    SELECT
        dd.associated_devices['camera_device_id'] AS device_id,
        enabled AS device_enabled,
        _approximate_creation_date_time AS device_timestamp
    FROM all_roadside_settings_device_us
    JOIN datamodel_core.dim_devices dd
        ON REGEXP_EXTRACT(EntityId, 'DEVICE#(.+)', 1) = CAST(dd.device_id AS STRING)
    WHERE
        Namespace = 'SafetyEventDetection'
        AND dd.date = '{PARTITION_START}'
        AND dd.associated_devices['camera_product_id'] IN (155, 167)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY EntityId ORDER BY _approximate_creation_date_time DESC) = 1
),
device_resolved_settings_us AS (
    -- Join devices to orgs and apply inheritance logic
    SELECT
        d.device_id,
        d.org_id,
        -- Use device-level value if not null, otherwise use org-level value
        COALESCE(ds.device_enabled, los.org_enabled) AS enabled,
        -- Use device-level timestamp if device has settings, otherwise org-level timestamp
        COALESCE(ds.device_timestamp, los.org_timestamp) AS effective_timestamp
    FROM total_active_brigid_us d
    LEFT OUTER JOIN latest_device_settings_us ds
        ON CAST(d.device_id AS STRING) = ds.device_id
    LEFT OUTER JOIN latest_org_settings_us los
        ON d.org_id = CAST(los.org_id AS BIGINT)
),
device_enabled_history_us AS (
    -- Find when each device first became enabled
    SELECT
        device_id,
        org_id,
        effective_timestamp,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY effective_timestamp) AS rn
    FROM device_resolved_settings_us
    WHERE
        enabled = true
),
first_enabled_per_device_us AS (
    SELECT
        device_id,
        org_id,
        MIN(effective_timestamp) AS enabled_at
    FROM device_enabled_history_us
    GROUP BY device_id, org_id
),
current_device_settings_us AS (
    -- Get current enabled state per device
    SELECT
        device_id,
        org_id,
        effective_timestamp AS last_updated
    FROM device_resolved_settings_us
    WHERE
        enabled = true
),
enabled_devices_us AS (
    SELECT DISTINCT
        cds.device_id,
        cds.org_id,
        FROM_UNIXTIME(fed.enabled_at / 1000, 'yyyy-MM-dd HH:mm:ss') AS enabled_at,
        FROM_UNIXTIME(cds.last_updated / 1000, 'yyyy-MM-dd HH:mm:ss') AS last_updated
    FROM current_device_settings_us cds
    JOIN first_enabled_per_device_us fed
        ON cds.device_id = fed.device_id
        AND cds.org_id = fed.org_id
),
active_us AS (
    SELECT
        tam.org_id,
        COUNT(DISTINCT CASE WHEN tam.l1 > 0 THEN tam.device_id END) AS eligible_l1,
        COUNT(DISTINCT CASE WHEN tam.l7 > 0 THEN tam.device_id END) AS eligible_l7,
        COUNT(DISTINCT CASE WHEN tam.l28 > 0 THEN tam.device_id END) AS eligible_l28,
        COUNT(DISTINCT CASE WHEN tam.p28 > 0 THEN tam.device_id END) AS eligible_p28,
        COUNT(DISTINCT CASE WHEN tam.p7 > 0 THEN tam.device_id END) AS eligible_p7,
        COALESCE(COUNT(DISTINCT CASE WHEN DATE(edu.enabled_at) <= '{PARTITION_START}' AND tam.l1 > 0 THEN tam.device_id END), 0) AS usage_l1,
        COALESCE(COUNT(DISTINCT CASE WHEN DATE(edu.enabled_at) <= '{PARTITION_START}' AND tam.l7 > 0 THEN tam.device_id END), 0) AS usage_l7,
        COALESCE(COUNT(DISTINCT CASE WHEN DATE(edu.enabled_at) <= '{PARTITION_START}' AND tam.l28 > 0 THEN tam.device_id END), 0) AS usage_l28,
        COALESCE(COUNT(DISTINCT CASE WHEN DATE(edu.enabled_at) <= '{PARTITION_START}' AND tam.p28 > 0 THEN tam.device_id END), 0) AS usage_p28,
        COALESCE(COUNT(DISTINCT CASE WHEN DATE(edu.enabled_at) <= '{PARTITION_START}' AND tam.p7 > 0 THEN tam.device_id END), 0) AS usage_p7
    FROM total_active_brigid_us tam
    LEFT OUTER JOIN enabled_devices_us edu
        ON tam.device_id = edu.device_id
        AND tam.org_id = edu.org_id
    GROUP BY 1
),
active_grouped_us AS (
    -- Usage will be 0 when at least half the active devices do not have the feature enabled, but will be the number of active devices in the timespan otherwise
    SELECT
        org_id,
        CASE WHEN eligible_l1 > 0 AND usage_l1 * 1.0 / eligible_l1 >= 0.5 THEN usage_l1 ELSE 0 END AS usage_l1,
        CASE WHEN eligible_l7 > 0 AND usage_l7 * 1.0 / eligible_l7 >= 0.5 THEN usage_l7 ELSE 0 END AS usage_l7,
        CASE WHEN eligible_l28 > 0 AND usage_l28 * 1.0 / eligible_l28 >= 0.5 THEN usage_l28 ELSE 0 END AS usage_l28,
        CASE WHEN eligible_p28 > 0 AND usage_p28 * 1.0 / eligible_p28 >= 0.5 THEN usage_p28 ELSE 0 END AS usage_p28,
        CASE WHEN eligible_p7 > 0 AND usage_p7 * 1.0 / eligible_p7 >= 0.5 THEN usage_p7 ELSE 0 END AS usage_p7
    FROM active_us
),
all_roadside_settings_org_eu AS (
    SELECT
        EntityId,
        Namespace,
        FROM_JSON(Settings, 'struct<vulnerableRoadUserCollisionWarningSettings:struct<audioAlerts:struct<enabled:boolean>, enabled:boolean, enabledCameraStreams:struct<front:boolean, left:boolean, rear:boolean, right:boolean, roadFacingDashcam:boolean>>>').vulnerableRoadUserCollisionWarningSettings.audioAlerts.enabled AS enabled,
        _approximate_creation_date_time
    FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/settings`
    WHERE
        Namespace LIKE 'SafetyEventDetection%'
        AND EntityId LIKE 'ORG#%'
        AND DATE(FROM_UNIXTIME(_approximate_creation_date_time / 1000)) <= '{PARTITION_START}'
),
all_roadside_settings_device_eu AS (
    SELECT
        EntityId,
        Namespace,
        FROM_JSON(Settings, 'struct<vulnerableRoadUserCollisionWarningSettings:struct<audioAlerts:struct<enabled:boolean>, enabled:boolean, enabledCameraStreams:struct<front:boolean, left:boolean, rear:boolean, right:boolean, roadFacingDashcam:boolean>>>').vulnerableRoadUserCollisionWarningSettings.audioAlerts.enabled AS enabled,
        _approximate_creation_date_time
    FROM delta.`s3://samsara-eu-dynamodb-delta-lake/table/settings`
    WHERE
        Namespace LIKE 'SafetyEventDetection%'
        AND EntityId LIKE 'DEVICE#%'
        AND DATE(FROM_UNIXTIME(_approximate_creation_date_time / 1000)) <= '{PARTITION_START}'
),
latest_org_settings_eu AS (
    -- Get the latest org-level settings per org
    SELECT
        REGEXP_EXTRACT(EntityId, 'ORG#(.+)', 1) AS org_id,
        enabled AS org_enabled,
        _approximate_creation_date_time AS org_timestamp
    FROM all_roadside_settings_org_eu
    WHERE Namespace = 'SafetyEventDetection'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY EntityId ORDER BY _approximate_creation_date_time DESC) = 1
),
latest_device_settings_eu AS (
    -- Get the latest device-level settings per device
    -- Settings stores VG device ID, so getting Brigid device ID from dim_devices
    SELECT
        dd.associated_devices['camera_device_id'] AS device_id,
        enabled AS device_enabled,
        _approximate_creation_date_time AS device_timestamp
    FROM all_roadside_settings_device_eu
    JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` dd
        ON REGEXP_EXTRACT(EntityId, 'DEVICE#(.+)', 1) = CAST(dd.device_id AS STRING)
    WHERE
        Namespace = 'SafetyEventDetection'
        AND dd.date = '{PARTITION_START}'
        AND dd.associated_devices['camera_product_id'] IN (155, 167)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY EntityId ORDER BY _approximate_creation_date_time DESC) = 1
),
device_resolved_settings_eu AS (
    -- Join devices to orgs and apply inheritance logic
    SELECT
        d.device_id,
        d.org_id,
        -- Use device-level value if not null, otherwise use org-level value
        COALESCE(ds.device_enabled, los.org_enabled) AS enabled,
        -- Use device-level timestamp if device has settings, otherwise org-level timestamp
        COALESCE(ds.device_timestamp, los.org_timestamp) AS effective_timestamp
    FROM total_active_brigid_eu d
    LEFT OUTER JOIN latest_device_settings_eu ds
        ON CAST(d.device_id AS STRING) = ds.device_id
    LEFT OUTER JOIN latest_org_settings_eu los
        ON d.org_id = CAST(los.org_id AS BIGINT)
),
device_enabled_history_eu AS (
    -- Find when each device first became enabled
    SELECT
        device_id,
        org_id,
        effective_timestamp,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY effective_timestamp) AS rn
    FROM device_resolved_settings_eu
    WHERE
        enabled = true
),
first_enabled_per_device_eu AS (
    SELECT
        device_id,
        org_id,
        MIN(effective_timestamp) AS enabled_at
    FROM device_enabled_history_eu
    GROUP BY device_id, org_id
),
current_device_settings_eu AS (
    -- Get current enabled state per device
    SELECT
        device_id,
        org_id,
        effective_timestamp AS last_updated
    FROM device_resolved_settings_eu
    WHERE
        enabled = true
),
enabled_devices_eu AS (
    SELECT DISTINCT
        cds.device_id,
        cds.org_id,
        FROM_UNIXTIME(fed.enabled_at / 1000, 'yyyy-MM-dd HH:mm:ss') AS enabled_at,
        FROM_UNIXTIME(cds.last_updated / 1000, 'yyyy-MM-dd HH:mm:ss') AS last_updated
    FROM current_device_settings_eu cds
    JOIN first_enabled_per_device_eu fed
        ON cds.device_id = fed.device_id
        AND cds.org_id = fed.org_id
),
active_eu AS (
    SELECT
        tam.org_id,
        COUNT(DISTINCT CASE WHEN tam.l1 > 0 THEN tam.device_id END) AS eligible_l1,
        COUNT(DISTINCT CASE WHEN tam.l7 > 0 THEN tam.device_id END) AS eligible_l7,
        COUNT(DISTINCT CASE WHEN tam.l28 > 0 THEN tam.device_id END) AS eligible_l28,
        COUNT(DISTINCT CASE WHEN tam.p28 > 0 THEN tam.device_id END) AS eligible_p28,
        COUNT(DISTINCT CASE WHEN tam.p7 > 0 THEN tam.device_id END) AS eligible_p7,
        COALESCE(COUNT(DISTINCT CASE WHEN DATE(edu.enabled_at) <= '{PARTITION_START}' AND tam.l1 > 0 THEN tam.device_id END), 0) AS usage_l1,
        COALESCE(COUNT(DISTINCT CASE WHEN DATE(edu.enabled_at) <= '{PARTITION_START}' AND tam.l7 > 0 THEN tam.device_id END), 0) AS usage_l7,
        COALESCE(COUNT(DISTINCT CASE WHEN DATE(edu.enabled_at) <= '{PARTITION_START}' AND tam.l28 > 0 THEN tam.device_id END), 0) AS usage_l28,
        COALESCE(COUNT(DISTINCT CASE WHEN DATE(edu.enabled_at) <= '{PARTITION_START}' AND tam.p28 > 0 THEN tam.device_id END), 0) AS usage_p28,
        COALESCE(COUNT(DISTINCT CASE WHEN DATE(edu.enabled_at) <= '{PARTITION_START}' AND tam.p7 > 0 THEN tam.device_id END), 0) AS usage_p7
    FROM total_active_brigid_eu tam
    LEFT OUTER JOIN enabled_devices_eu edu
        ON tam.device_id = edu.device_id
        AND tam.org_id = edu.org_id
    GROUP BY 1
),
active_grouped_eu AS (
    -- Usage will be 0 when at least half the active devices do not have the feature enabled, but will be the number of active devices in the timespan otherwise
    SELECT
        org_id,
        CASE WHEN eligible_l1 > 0 AND usage_l1 * 1.0 / eligible_l1 >= 0.5 THEN usage_l1 ELSE 0 END AS usage_l1,
        CASE WHEN eligible_l7 > 0 AND usage_l7 * 1.0 / eligible_l7 >= 0.5 THEN usage_l7 ELSE 0 END AS usage_l7,
        CASE WHEN eligible_l28 > 0 AND usage_l28 * 1.0 / eligible_l28 >= 0.5 THEN usage_l28 ELSE 0 END AS usage_l28,
        CASE WHEN eligible_p28 > 0 AND usage_p28 * 1.0 / eligible_p28 >= 0.5 THEN usage_p28 ELSE 0 END AS usage_p28,
        CASE WHEN eligible_p7 > 0 AND usage_p7 * 1.0 / eligible_p7 >= 0.5 THEN usage_p7 ELSE 0 END AS usage_p7
    FROM active_eu
),
active_grouped AS (
    SELECT *
    FROM active_grouped_us

    UNION ALL

    SELECT *
    FROM active_grouped_eu
),
eligible_orgs AS (
    -- Incab is a subset of event detection, so only looking at orgs that met the event detection criteria from earlier
    SELECT DISTINCT
        org_id
    FROM product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_cm_global
    WHERE
        date = '{PARTITION_START}'
        AND usage_weekly > 0
)
-- Definitions:
-- usage_weekly: Usage in the last 7 days (enabled for pedestrian collision warning incab)
-- usage_monthly: Usage in the last 28 days (enabled for pedestrian collision warning incab)
-- usage_prior_month: Usage in the 28 days prior to the last 28 days (enabled for pedestrian collision warning incab)
-- usage_weekly_prior_month: Usage in the 7 days prior to the last 28 days (enabled for pedestrian collision warning incab)
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
    COALESCE(e.usage_l7, 0) AS usage_weekly,
    COALESCE(e.usage_l28, 0) AS usage_monthly,
    COALESCE(e.usage_p28, 0) AS usage_prior_month,
    COALESCE(e.usage_p7, 0) AS usage_weekly_prior_month,
    CAST(0 AS BIGINT) AS daily_active_user,
    CAST(0 AS BIGINT) AS daily_enabled_users,
    CAST(0 AS BIGINT) AS weekly_active_users,
    CAST(0 AS BIGINT) AS weekly_enabled_users,
    CAST(0 AS BIGINT) AS monthly_active_users,
    CAST(0 AS BIGINT) AS monthly_enabled_users,
    CASE WHEN COALESCE(e.usage_l1, 0) > 0 THEN 1 ELSE 0 END AS org_active_day,
    CASE WHEN COALESCE(e.usage_l7, 0) > 0 THEN 1 ELSE 0 END AS org_active_week,
    CASE WHEN COALESCE(e.usage_p7, 0) > 0 THEN 1 ELSE 0 END AS org_active_week_prior_month,
    CASE WHEN COALESCE(e.usage_l28, 0) > 0 THEN 1 ELSE 0 END AS org_active_month,
    CASE WHEN COALESCE(e.usage_p28, 0) > 0 THEN 1 ELSE 0 END AS org_active_prior_month
FROM final_relevant_orgs o
JOIN eligible_orgs eo
    ON o.org_id = eo.org_id
LEFT OUTER JOIN active_grouped e
    ON e.org_id = o.org_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""(Global) As part of Executive Scorecard, this shows usage stats for the Pedestrian Collision Warning (CM Incab) feature""",
        row_meaning="""Each row represents an org's usage of Pedestrian Collision Warning (CM Incab) over various time periods""",
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
        NonEmptyDQCheck(name="dq_non_empty_agg_executive_scorecard_pedestrian_collision_warning_cm_incab_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_agg_executive_scorecard_pedestrian_collision_warning_cm_incab_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_agg_executive_scorecard_pedestrian_collision_warning_cm_incab_global",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
        TrendDQCheck(name="dq_trend_agg_executive_scorecard_pedestrian_collision_warning_cm_incab_global_enablement", lookback_days=1, tolerance=0.02),
    ],
    backfill_batch_size=1,
    upstreams=[
        "us-west-2|eu-west-1:dynamodb.settings",
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1:datamodel_core.dim_devices",
        "us-west-2|eu-west-1:datamodel_core.lifetime_device_activity",
        "us-west-2|eu-west-1:product_analytics_staging.dim_organizations_classification",
        "us-west-2|eu-west-1:finopsdb.customer_info",
        "us-west-2:datamodel_launchdarkly_bronze.segment_flag",
        "us-west-2:datamodel_launchdarkly_bronze.rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause",
        "us-west-2:datamodel_launchdarkly_bronze.feature_flag_variation",
        "us-west-2:release_management.ld_flag_variation_index_map",
        "us-west-2|eu-west-1:clouddb.organizations",
        "us-west-2:product_analytics_staging.agg_executive_scorecard_pedestrian_collision_warning_cm_global",
    ],
)
def agg_executive_scorecard_pedestrian_collision_warning_cm_incab_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_executive_scorecard_pedestrian_collision_warning_cm_incab_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    json_config = load_product_usage_config("pedestrian_collision_warning_cm_incab")
    relevant_orgs = get_final_relevant_orgs(json_config, PARTITION_START)
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        RELEVANT_ORGS=relevant_orgs,
    )
    context.log.info(f"{query}")
    return query
