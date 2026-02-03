from dagster import AssetKey, Backoff, DailyPartitionsDefinition, Jitter, RetryPolicy

# Schemas need to be separated into different files due to CI/CD limitations on file size.
from ...assets.core_dimensions.schemas import (
    dim_devices_settings,
    stg_devices_settings_audio_alerts,
    stg_devices_settings_first_on_date,
    stg_devices_settings_latest_date,
)
from ...common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    JoinableDQCheck,
    NonEmptyDQCheck,
    PrimaryKeyDQCheck,
    TableType,
    TrendDQCheck,
    WarehouseWriteMode,
    apply_db_overrides,
    build_assets_from_sql,
    build_table_description,
    get_all_regions,
)

key_prefix = "datamodel_core"
databases = {
    "database_bronze": Database.DATAMODEL_CORE_BRONZE,
    "database_silver": Database.DATAMODEL_CORE_SILVER,
    "database_gold": Database.DATAMODEL_CORE,
}
database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
}
databases = apply_db_overrides(databases, database_dev_overrides)

pipeline_group_name = "dim_devices_settings"
# end_offset=1 specifies that the final partition is the one ending at 00:00 the day after the current day.
# start_date is based on the earliest data available in upstream dependency dim_organizations
daily_partition_def = DailyPartitionsDefinition(start_date="2024-01-01", end_offset=1)

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

# A RetryPolicy is specified to handle transient S3 file reading errors (FAILED_READ_FILE.NO_HINT) that can occur
# when reading Parquet files from s3://samsara-datamodel-warehouse/ during the GROUP BY operations.
dim_devices_settings_retry_policy = RetryPolicy(
    max_retries=3,  # Maximum number of retries
    delay=120,  # Initial delay in seconds
    backoff=Backoff.EXPONENTIAL,  # Delay multiplier for each retry
    jitter=Jitter.FULL,  # Apply randomness to delay intervals
)

stg_devices_settings_first_on_date_schema = stg_devices_settings_first_on_date.schema
stg_devices_settings_first_on_date_query = """--sql
    -- For each day, this query gets the first config of the day.
    SELECT
        -- This date will be the partition date {DATEID} unless the filter is changed.
        '{DATEID}' AS `date`,
        org_id,
        object_id AS device_id,
        MIN(`time`) AS `time`,
        MIN_BY(s3_proto_value.reported_device_config.device_config.public_product_name, `time`) AS public_product_name,
        MIN_BY(s3_proto_value.reported_device_config.firmware_build, `time`) AS firmware_build,
        MIN_BY(s3_proto_value.reported_device_config.device_config, `time`) AS device_config
    FROM kinesisstats_history.osdreporteddeviceconfig_with_s3_big_stat
    WHERE `date` = '{DATEID}'
    GROUP BY org_id, object_id
--endsql"""

stg_devices_settings_first_on_date_assets = build_assets_from_sql(
    name="stg_devices_settings_first_on_date",
    schema=stg_devices_settings_first_on_date_schema,
    description="""A staging table that contains the first config for each device for a given day""",
    sql_query=stg_devices_settings_first_on_date_query,
    primary_keys=[
        "date",
        "org_id",
        "device_id",
    ],
    upstreams=[
        AssetKey(["kinesisstats_history", "osdreporteddeviceconfig_with_s3_big_stat"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
    step_launcher="databricks_pyspark_step_launcher_devices_settings",
)

stg_devices_settings_first_on_date_us = stg_devices_settings_first_on_date_assets[
    AWSRegions.US_WEST_2.value
]
stg_devices_settings_first_on_date_eu = stg_devices_settings_first_on_date_assets[
    AWSRegions.EU_WEST_1.value
]
stg_devices_settings_first_on_date_ca = stg_devices_settings_first_on_date_assets[
    AWSRegions.CA_CENTRAL_1.value
]

dqs["stg_devices_settings_first_on_date"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_devices_settings_first_on_date",
        table="stg_devices_settings_first_on_date",
        primary_keys=["date", "org_id", "device_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

dqs["stg_devices_settings_first_on_date"].append(
    NonEmptyDQCheck(
        name="dq_empty_stg_devices_settings_first_on_date",
        table="stg_devices_settings_first_on_date",
        blocking=True,
        database=databases["database_silver_dev"],
    )
)


stg_devices_settings_latest_date_schema = stg_devices_settings_latest_date.schema
stg_devices_settings_latest_date_query = """--sql
    -- From the staging table stg_devices_settings_first_on_date,
    -- finds the most recent date with a config for a device <org_id, device_id> and returns that config.
    WITH latest_dates AS (
        -- This CTE isolates the MAX() function to the date column, in order to limit
        -- the number of times the device_config column is read into memory, since the massive
        -- size of this column results in frequent OOM errors when it has to be read in.
        SELECT
            '{DATEID}' AS date,
            MAX(date) AS latest_config_date,
            org_id,
            device_id
        FROM {database_silver_dev}.stg_devices_settings_first_on_date
        -- The lookback window is the last 182 days in the staging table.
        -- Previously, this window was longer (365+ days), but was reduced to 182 days to minimize OOM errors.
        WHERE date <= '{DATEID}'
            AND date >= CAST(DATE_SUB('{DATEID}', 182) AS STRING)
        GROUP BY org_id, device_id
    )
    SELECT
        -- This table is partitioned on the date on which the lookback ends ({DATEID});
        -- e.g., if run for 2024-04-30, for a given <org_id, device_id> the latest_config_date and
        -- latest_config_time capture the date and time of the most recent config -
        -- latest_config_date = 2024-04-28 and date = 2024-04-30.
        ld.date,
        ld.latest_config_date,
        ld.org_id,
        ld.device_id,
        fd.time AS latest_config_time,
        fd.public_product_name,
        fd.firmware_build,
        fd.device_config
    FROM latest_dates ld
    INNER JOIN {database_silver_dev}.stg_devices_settings_first_on_date fd
        ON ld.latest_config_date = fd.date
        AND ld.org_id = fd.org_id
        AND ld.device_id = fd.device_id
    -- Explicit partition filter to enable Spark partition pruning.
    -- Without this, Spark scans all partitions because it cannot push down
    -- the join predicate (ld.latest_config_date) for partition pruning.
    WHERE fd.date >= CAST(DATE_SUB('{DATEID}', 182) AS STRING)
        AND fd.date <= '{DATEID}'
--endsql"""

stg_devices_settings_latest_date_assets = build_assets_from_sql(
    name="stg_devices_settings_latest_date",
    schema=stg_devices_settings_latest_date_schema,
    description="""A staging table that gets the most recent date with a config for a given <org_id, device_id> combo""",
    sql_query=stg_devices_settings_latest_date_query,
    primary_keys=[
        "date",
        "org_id",
        "device_id",
    ],
    upstreams=[
        AssetKey(
            [
                databases["database_silver_dev"],
                "dq_stg_devices_settings_first_on_date",
            ]
        ),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
    step_launcher="databricks_pyspark_step_launcher_devices_settings",
    retry_policy=dim_devices_settings_retry_policy,
)

stg_devices_settings_latest_date_us = stg_devices_settings_latest_date_assets[
    AWSRegions.US_WEST_2.value
]
stg_devices_settings_latest_date_eu = stg_devices_settings_latest_date_assets[
    AWSRegions.EU_WEST_1.value
]
stg_devices_settings_latest_date_ca = stg_devices_settings_latest_date_assets[
    AWSRegions.CA_CENTRAL_1.value
]

dqs["stg_devices_settings_latest_date"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_devices_settings_latest_date",
        table="stg_devices_settings_latest_date",
        primary_keys=["date", "org_id", "device_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

dqs["stg_devices_settings_latest_date"].append(
    NonEmptyDQCheck(
        name="dq_empty_stg_devices_settings_latest_date",
        table="stg_devices_settings_latest_date",
        blocking=True,
        database=databases["database_silver_dev"],
    )
)


stg_devices_settings_audio_alerts_schema = stg_devices_settings_audio_alerts.schema
stg_devices_settings_audio_alerts_query = """--sql
    -- Explode the array of alert settings in order to identify each type individually
    WITH explode_audio_alerts AS (
        SELECT
            `date`, latest_config_date, org_id, device_id, latest_config_time, public_product_name, firmware_build,
            EXPLODE(device_config.audio_alerts.audio_alert_configs) AS audio_alert_configs
        FROM {database_silver_dev}.stg_devices_settings_latest_date
        WHERE `date` = '{DATEID}'
    )
    , label_audio_alerts AS (
        SELECT
            `date`, latest_config_date, org_id, device_id, latest_config_time, public_product_name, firmware_build,
            CASE
                WHEN audio_alert_configs.event_type = 0 THEN 'UNKNOWN_EVENT'
                WHEN audio_alert_configs.event_type = 1 THEN 'GENERIC_EVENT'
                WHEN audio_alert_configs.event_type = 2 THEN 'NEAR_COLLISION_EVENT'
                WHEN audio_alert_configs.event_type = 3 THEN 'DISTRACTED_DRIVING_EVENT'
                WHEN audio_alert_configs.event_type = 4 THEN 'DDD_CANCELLATION_EVENT'
                WHEN audio_alert_configs.event_type = 5 THEN 'SPEEDING_ALERT_EVENT'
                WHEN audio_alert_configs.event_type = 6 THEN 'TAILGATING_EVENT'
                WHEN audio_alert_configs.event_type = 7 THEN 'ROLLING_STOP_EVENT'
                WHEN audio_alert_configs.event_type = 8 THEN 'RAILROAD_CROSSING_EVENT'
                WHEN audio_alert_configs.event_type = 9 THEN 'PHONE_EVENT'
                WHEN audio_alert_configs.event_type = 10 THEN 'FOOD_EVENT'
                WHEN audio_alert_configs.event_type = 11 THEN 'DRINK_EVENT'
                WHEN audio_alert_configs.event_type = 12 THEN 'SMOKING_EVENT'
                WHEN audio_alert_configs.event_type = 13 THEN 'SEATBELT_EVENT'
                WHEN audio_alert_configs.event_type = 14 THEN 'MASK_EVENT'
                WHEN audio_alert_configs.event_type = 15 THEN 'OBSTRUCTION_EVENT'
                WHEN audio_alert_configs.event_type = 16 THEN 'PASSENGER_EVENT'
                WHEN audio_alert_configs.event_type = 17 THEN 'LIVESTREAM_START_EVENT'
                WHEN audio_alert_configs.event_type = 18 THEN 'LIVESTREAM_END_EVENT'
                WHEN audio_alert_configs.event_type = 19 THEN 'FALSE_NEGATIVE'
                WHEN audio_alert_configs.event_type = 20 THEN 'VG_GENERATED_CRASH_EVENT'
                WHEN audio_alert_configs.event_type = 21 THEN 'VG_GENERATED_OBD_SPEED_EVENT'
                -- Used for audio alerts triggered by haEvents that occur due to failed classification
                WHEN audio_alert_configs.event_type = 22 THEN 'VG_GENERATED_HARSH_DRIVING_EVENT'
                WHEN audio_alert_configs.event_type = 23 THEN 'VG_GENERATED_OBD_SEATBELT_EVENT'
                WHEN audio_alert_configs.event_type = 24 THEN 'CALL_DISPATCH_EVENT'
                WHEN audio_alert_configs.event_type = 25 THEN 'CM_STARTUP_EVENT'
                WHEN audio_alert_configs.event_type = 26 THEN 'VG_GENERATED_HARSH_ACCEL_EVENT'
                WHEN audio_alert_configs.event_type = 27 THEN 'VG_GENERATED_HARSH_BRAKE_EVENT'
                WHEN audio_alert_configs.event_type = 28 THEN 'VG_GENERATED_HARSH_TURN_EVENT'
                WHEN audio_alert_configs.event_type = 29 THEN 'VG_GENERATED_ROLLOVER_PROTECTION_ENGINE_CONTROL_ACTIVATED_EVENT'
                WHEN audio_alert_configs.event_type = 30 THEN 'VG_GENERATED_ROLLOVER_PROTECTION_BRAKE_CONTROL_ACTIVATED_EVENT'
                WHEN audio_alert_configs.event_type = 31 THEN 'VG_GENERATED_YAW_CONTROL_ENGINE_CONTROL_ACTIVATED_EVENT'
                WHEN audio_alert_configs.event_type = 32 THEN 'VG_GENERATED_YAW_CONTROL_BRAKE_CONTROL_ACTIVATED_EVENT'
                -- Audio feedback successful QRCode scans
                WHEN audio_alert_configs.event_type = 33 THEN 'QRCODE_SCAN'
                WHEN audio_alert_configs.event_type = 34 THEN 'DRIVERSIGNIN_REQUEST'
                WHEN audio_alert_configs.event_type = 35 THEN 'DRIVERSIGNIN_SUCCESS'
                WHEN audio_alert_configs.event_type = 36 THEN 'IMMOBILIZE_ENGINE_ON_TRIP_WARNING'
                WHEN audio_alert_configs.event_type = 37 THEN 'CAMERA_BUTTON_SINGLE_PRESS'
                WHEN audio_alert_configs.event_type = 38 THEN 'LANE_DEPARTURE_EVENT'
                WHEN audio_alert_configs.event_type = 39 THEN 'CM_RECORDING_STOP_EVENT'
                WHEN audio_alert_configs.event_type = 40 THEN 'DROWSINESS_EVENT'
            END AS alert_type,
            IFNULL(audio_alert_configs.enabled, FALSE) AS alerts_enabled
        FROM explode_audio_alerts
    )
    -- Convert the audio alert labels to harsh event types.
    -- This mapping comes from AI/ML work here: https://github.com/samsara-dev/mlcv_projects/blob/master/projects/ray-projects/pang-workspace/ray/pang_workspace_ray_jobs/deviceconfig_utils/constants.py.
    -- This mapping only includes alerts for certain event types that AI/ML is interested in.
    -- More audio alert_type to harsh event type mappings can be added as demand requires.
    , audio_alerts_to_event_types AS (
        SELECT
            `date`, latest_config_date, org_id, device_id, latest_config_time, public_product_name, firmware_build,
            CASE
                WHEN alert_type = 'NEAR_COLLISION_EVENT' THEN ARRAY('haNearCollision')
                WHEN alert_type = 'DISTRACTED_DRIVING_EVENT' THEN ARRAY('haDistractedDriving')
                WHEN alert_type = 'TAILGATING_EVENT' THEN ARRAY('haTailgating')
                WHEN alert_type = 'ROLLING_STOP_EVENT' THEN ARRAY('haTileRollingStopSign', 'haRolledStopSign')
                WHEN alert_type = 'RAILROAD_CROSSING_EVENT' THEN ARRAY('haTileRollingRailroadCrossing')
                WHEN alert_type = 'PHONE_EVENT' THEN ARRAY('haPhonePolicy')
                WHEN alert_type = 'FOOD_EVENT' THEN ARRAY('haFoodPolicy')
                WHEN alert_type = 'DRINK_EVENT' THEN ARRAY('haDrinkPolicy')
                WHEN alert_type = 'SMOKING_EVENT' THEN ARRAY('haSmokingPolicy')
                WHEN alert_type = 'SEATBELT_EVENT' THEN ARRAY('haSeatbeltPolicy')
                WHEN alert_type = 'MASK_EVENT' THEN ARRAY('haMaskPolicy')
                WHEN alert_type = 'OBSTRUCTION_EVENT' THEN ARRAY('haDriverObstructionPolicy', 'haOutwardObstructionPolicy')
                WHEN alert_type = 'VG_GENERATED_HARSH_ACCEL_EVENT' THEN ARRAY('haAccel')
                WHEN alert_type = 'VG_GENERATED_HARSH_BRAKE_EVENT' THEN ARRAY('haBraking')
                WHEN alert_type = 'VG_GENERATED_HARSH_TURN_EVENT' THEN ARRAY('haSharpTurn')
                WHEN alert_type = 'VG_GENERATED_CRASH_EVENT' THEN ARRAY('haCrash')
                WHEN alert_type = 'LANE_DEPARTURE_EVENT' THEN ARRAY('haLaneDeparture')
                WHEN alert_type = 'DROWSINESS_EVENT' THEN ARRAY('haDrowsinessDetection')
                ELSE NULL
            END AS harsh_event_name_array,
            alerts_enabled
        FROM label_audio_alerts
    )
    -- Audio alerts must be exploded out because a single alert type could cover multiple
    -- harsh event types; e.g., ROLLING_STOP_EVENT -> ('haTileRollingStopSign', 'haRolledStopSign')
    , explode_event_types_alerts AS (
        SELECT
            `date`, latest_config_date, org_id, device_id, latest_config_time, public_product_name, firmware_build,
            EXPLODE(harsh_event_name_array) AS harsh_event_name,
            -- If anyone of the severity levels is enabled, consider the alert enabled.
            MAX(alerts_enabled) alerts_enabled
        FROM audio_alerts_to_event_types
        -- Groups alert types with duplicates (multiple alert entries for severity levels)
        GROUP BY 1,2,3,4,5,6,7,8
    )
    -- Group alert types into a single struct
    , combine_alert_types AS (
        SELECT
            `date`, latest_config_date, org_id, device_id, latest_config_time, public_product_name, firmware_build,
            COLLECT_SET(
                NAMED_STRUCT(
                    'harsh_event_name', harsh_event_name,
                    'alerts_enabled', alerts_enabled
                )
            ) AS audio_alerts
        FROM explode_event_types_alerts
        GROUP BY 1,2,3,4,5,6,7
    )
    -- Transform array (from COLLECT_SET) into a map that can be indexed by the key harsh_event_name,
    -- so that these alerts can be joined to the other device harsh event settings.
    SELECT `date`, latest_config_date, org_id, device_id, latest_config_time, public_product_name, firmware_build,
        MAP_FROM_ARRAYS(audio_alerts.harsh_event_name, audio_alerts.alerts_enabled) AS audio_alerts
    FROM combine_alert_types
--endsql"""

stg_devices_settings_audio_alerts_assets = build_assets_from_sql(
    name="stg_devices_settings_audio_alerts",
    schema=stg_devices_settings_audio_alerts_schema,
    description="""A staging table containing device setting for harsh audio alerts""",
    sql_query=stg_devices_settings_audio_alerts_query,
    primary_keys=[
        "date",
        "org_id",
        "device_id",
    ],
    upstreams=[
        AssetKey(
            [
                databases["database_silver_dev"],
                "dq_stg_devices_settings_latest_date",
            ]
        ),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
    depends_on_past=True,  # Query will return misleading results if run when previous dates have not been materialized
)

stg_devices_settings_audio_alerts_us = stg_devices_settings_audio_alerts_assets[
    AWSRegions.US_WEST_2.value
]
stg_devices_settings_audio_alerts_eu = stg_devices_settings_audio_alerts_assets[
    AWSRegions.EU_WEST_1.value
]
stg_devices_settings_audio_alerts_ca = stg_devices_settings_audio_alerts_assets[
    AWSRegions.CA_CENTRAL_1.value
]

dqs["stg_devices_settings_audio_alerts"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_devices_settings_audio_alerts",
        table="stg_devices_settings_audio_alerts",
        primary_keys=["date", "org_id", "device_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)


dim_devices_settings_schema = dim_devices_settings.schema
dim_devices_settings_query = """--sql
    -- Some of the business questions this table can be used to answer:
    -- - How have the device configuration(s) for an org changed over time?
    -- - What share of an org's devices have a certain setting enabled?
    SELECT
        cfg.`date`, cfg.latest_config_date, cfg.org_id, cfg.device_id,
        cfg.latest_config_time, cfg.public_product_name, cfg.firmware_build,
        NAMED_STRUCT(
            -- Legacy settings not used by latest CM devices (Brigid)
            --
            -- Current harsh event detection framework is mlapp
            -- Enabled/Disabled flags specified here:
            -- https://github.com/samsara-dev/mlcv_projects/blob/master/packages/aiml-backend-proto/src/samsaradev/io/hubproto/enabled_flag_pb2.pyi
            --
            -- ddd = haDistractedDriving
            'distracted_driving_detection',
                NAMED_STRUCT(
                    'harsh_event_name', 'haDistractedDriving',
                    'legacy_enabled',
                    CASE
                        WHEN cfg.public_product_name IN ('CM33', 'CM34') THEN FALSE
                        WHEN device_config.ddd IS NULL THEN FALSE
                        ELSE device_config.ddd.enabled = 1
                    END,
                    'enabled',
                    CASE
                        WHEN cfg.public_product_name IN ('CM33', 'CM34') THEN device_config.inward_safety_mlapp_config.inattentive_driving_config.enabled = 1
                        WHEN device_config.inward_safety_mlapp_config.inattentive_driving_config.enabled IS NULL THEN FALSE
                        ELSE device_config.inward_safety_mlapp_config.inattentive_driving_config.enabled = 1
                    END,
                    'audio_alerts_enabled', aa.audio_alerts.haDistractedDriving
                ),
            -- hac (hand action classifier) = mud (mobile usage device) = haPhonePolicy
            'mobile_usage',
                NAMED_STRUCT(
                    'harsh_event_name', 'haPhonePolicy',
                    'legacy_enabled',
                    CASE
                        WHEN cfg.public_product_name IN ('CM33', 'CM34') THEN FALSE
                        WHEN device_config.distracted_policy_detector_config.phone_policy_config.enabled IS NULL THEN FALSE
                        ELSE device_config.distracted_policy_detector_config.phone_policy_config.enabled = 1
                    END,
                    'enabled',
                    CASE
                        WHEN device_config.inward_safety_mlapp_config.mobile_usage_config.enabled IS NULL THEN FALSE
                        ELSE device_config.inward_safety_mlapp_config.mobile_usage_config.enabled = 1
                    END,
                    'audio_alerts_enabled', aa.audio_alerts.haPhonePolicy
                ),
            -- sbc (seatbelt classifier) = haSeatbeltPolicy
            'seatbelt',
                NAMED_STRUCT(
                    'harsh_event_name', 'haSeatbeltPolicy',
                    'legacy_enabled',
                    CASE
                        WHEN cfg.public_product_name IN ('CM33', 'CM34') THEN FALSE
                        WHEN device_config.distracted_policy_detector_config.seatbelt_config.enabled IS NULL THEN FALSE
                        ELSE device_config.distracted_policy_detector_config.seatbelt_config.enabled = 1
                    END,
                    'enabled',
                    CASE
                        WHEN device_config.inward_safety_mlapp_config.seatbelt_usage_config.enabled IS NULL THEN FALSE
                        ELSE device_config.inward_safety_mlapp_config.seatbelt_usage_config.enabled = 1
                    END,
                    'audio_alerts_enabled', aa.audio_alerts.haSeatbeltPolicy
                ),
            -- fcw (forward collision warning) = haNearCollision
            'forward_collision_warning',
                NAMED_STRUCT(
                    'harsh_event_name', 'haNearCollision',
                    'legacy_enabled',
                    CASE
                        WHEN cfg.public_product_name IN ('CM33', 'CM34') THEN FALSE
                        WHEN device_config.vision.time_to_collision IS NULL THEN FALSE
                        ELSE device_config.vision.time_to_collision.EnableFeature AND device_config.vision.time_to_collision.alert.EnableAudioAlerts
                    END,
                    'enabled',
                    -- _descriptor.EnumValueDescriptor(
                    --     name='RunPeriodWhileOnTrip', index=3, number=3,
                    CASE
                        WHEN device_config.forward_collision_warning.mlapp_config.run_control_config.run_period IS NULL THEN FALSE
                        ELSE device_config.forward_collision_warning.mlapp_config.run_control_config.run_period = 3
                    END,
                    'audio_alerts_enabled', aa.audio_alerts.haNearCollision,
                    'shadow_events_enabled',
                    CASE
                        WHEN device_config.forward_collision_warning.mlapp_config.shadow_mode_config.shadow_events_enabled IS NULL THEN FALSE
                        ELSE device_config.forward_collision_warning.mlapp_config.shadow_mode_config.shadow_events_enabled = 1
                    END,
                    'shadow_events_alerts_enabled',
                    CASE
                        WHEN device_config.forward_collision_warning.mlapp_config.shadow_mode_config.audio_alerts_enabled IS NULL THEN FALSE
                        ELSE device_config.forward_collision_warning.mlapp_config.shadow_mode_config.audio_alerts_enabled = 1
                    END,
                    'model_registry_key',
                    CASE
                        WHEN device_config.forward_collision_warning.mlapp_config.model_config.model_registry_key = '' THEN NULL
                        ELSE device_config.forward_collision_warning.mlapp_config.model_config.model_registry_key
                    END
                ),
            -- fd (following distance) = haTailgating
            'following_distance',
                NAMED_STRUCT(
                    'harsh_event_name', 'haTailgating',
                    'legacy_enabled',
                    CASE
                        WHEN device_config.vision.tailgating_estimation IS NULL THEN FALSE
                        ELSE device_config.vision.tailgating_estimation.EnableFeature
                    END,
                    'enabled',
                    CASE
                        WHEN device_config.outward_safety_mlapp.following_distance_config.enabled IS NULL THEN FALSE
                        ELSE device_config.outward_safety_mlapp.following_distance_config.enabled = 1
                    END,
                    'audio_alerts_enabled', aa.audio_alerts.haTailgating
                ),
            -- ldw = lane departure warning = haLaneDeparture
            'lane_departure_warning',
                NAMED_STRUCT(
                    'harsh_event_name', 'haLaneDeparture',
                    'enabled',
                    CASE
                        WHEN device_config.outward_safety_mlapp.lane_departure_warning_config.enabled IS NULL THEN FALSE
                        ELSE device_config.outward_safety_mlapp.lane_departure_warning_config.enabled = 1
                    END,
                    'audio_alerts_enabled', aa.audio_alerts.haLaneDeparture
                ),
            -- d3 = drowsiness detection  = haDrowsinessDetection
            'drowsiness',
                NAMED_STRUCT(
                    'harsh_event_name', 'haDrowsinessDetection',
                    'enabled',
                    -- // Always run the MLApp.
                    -- RunControlConfig_RunPeriodAlways RunControlConfig_RunPeriod = 2
                    -- // Run the MLApp while on a firmware defined trip.
                    -- RunControlConfig_RunPeriodWhileOnTrip RunControlConfig_RunPeriod = 3
                    CASE
                        WHEN device_config.drowsiness_mlapp_config.ml_app_config.run_control_config.run_period IS NULL THEN FALSE
                        ELSE device_config.drowsiness_mlapp_config.ml_app_config.run_control_config.run_period = 2 OR device_config.drowsiness_mlapp_config.ml_app_config.run_control_config.run_period = 3
                    END,
                    'audio_alerts_enabled', aa.audio_alerts.haDrowsinessDetection,
                    'shadow_events_enabled',
                    CASE
                        WHEN device_config.drowsiness_mlapp_config.ml_app_config.shadow_mode_config.shadow_events_enabled IS NULL THEN FALSE
                        ELSE device_config.drowsiness_mlapp_config.ml_app_config.shadow_mode_config.shadow_events_enabled = 1
                    END,
                    'shadow_events_alerts_enabled',
                    CASE
                        WHEN device_config.drowsiness_mlapp_config.ml_app_config.shadow_mode_config.audio_alerts_enabled IS NULL THEN FALSE
                        ELSE device_config.drowsiness_mlapp_config.ml_app_config.shadow_mode_config.audio_alerts_enabled = 1
                    END,
                    'model_registry_key',
                    CASE
                        WHEN device_config.drowsiness_mlapp_config.ml_app_config.model_config.model_registry_key = '' THEN NULL
                        ELSE device_config.drowsiness_mlapp_config.ml_app_config.model_config.model_registry_key
                    END
                ),
            -- inward_safety = ("haDistractedDriving", "haPhonePolicy", "haSeatbeltPolicy",)
            'inward_safety',
                NAMED_STRUCT(
                    'shadow_events_enabled',
                    CASE
                        WHEN device_config.inward_safety_mlapp_config.mlapp_config.shadow_mode_config.shadow_events_enabled IS NULL THEN FALSE
                        ELSE device_config.inward_safety_mlapp_config.mlapp_config.shadow_mode_config.shadow_events_enabled = 1
                    END,
                    'shadow_events_alerts_enabled',
                    CASE
                        WHEN device_config.inward_safety_mlapp_config.mlapp_config.shadow_mode_config.audio_alerts_enabled IS NULL THEN FALSE
                        ELSE device_config.inward_safety_mlapp_config.mlapp_config.shadow_mode_config.audio_alerts_enabled = 1
                    END,
                    'model_registry_key',
                    CASE
                        WHEN device_config.inward_safety_mlapp_config.mlapp_config.model_config.model_registry_key = '' THEN NULL
                        ELSE device_config.inward_safety_mlapp_config.mlapp_config.model_config.model_registry_key
                    END
                ),
            -- outward_safety = ("haTailgating", "haLaneDeparture",)
            'outward_safety',
                NAMED_STRUCT(
                    'shadow_events_enabled',
                    CASE
                        WHEN device_config.outward_safety_mlapp.mlapp_config.shadow_mode_config.shadow_events_enabled IS NULL THEN FALSE
                        ELSE device_config.outward_safety_mlapp.mlapp_config.shadow_mode_config.shadow_events_enabled = 1
                    END,
                    'shadow_events_alerts_enabled',
                    CASE
                        WHEN device_config.outward_safety_mlapp.mlapp_config.shadow_mode_config.audio_alerts_enabled IS NULL THEN FALSE
                        ELSE device_config.outward_safety_mlapp.mlapp_config.shadow_mode_config.audio_alerts_enabled = 1
                    END,
                    'model_registry_key',
                    CASE
                        WHEN device_config.outward_safety_mlapp.mlapp_config.model_config.model_registry_key = '' THEN NULL
                        ELSE device_config.outward_safety_mlapp.mlapp_config.model_config.model_registry_key
                    END
                ),
            -- Rolling stop settings (rs = rolling stop)
            --
            'tile_rolling_stop',
                NAMED_STRUCT(
                    'harsh_event_name', 'haTileRollingStopSign',
                    'enabled',
                    CASE
                        WHEN device_config.tile_rolling_stop.stop_sign_policy.enabled IS NULL THEN FALSE
                        ELSE device_config.tile_rolling_stop.stop_sign_policy.enabled
                    END,
                    'audio_alerts_enabled', aa.audio_alerts.haTileRollingStopSign
                ),
            -- CV based rolling stop
            'rolling_stop',
                NAMED_STRUCT(
                    'harsh_event_name', 'haRolledStopSign',
                    'enabled',
                    CASE
                        WHEN device_config.rolling_stop.v1 IS NULL THEN FALSE
                        ELSE TRUE
                    END,
                    'audio_alerts_enabled', aa.audio_alerts.haRolledStopSign
                ),
            -- Obstructions
            --
            -- io = inward obstruction = driver obstruction = haDriverObstructionPolicy
            'inward_obstruction',
                NAMED_STRUCT(
                    'harsh_event_name', 'haDriverObstructionPolicy',
                    'enabled',
                    CASE
                        WHEN device_config.distracted_policy_detector_config.driver_obstruction_config.enabled IS NULL THEN FALSE
                        ELSE device_config.distracted_policy_detector_config.driver_obstruction_config.enabled
                    END,
                    'audio_alerts_enabled', aa.audio_alerts.haDriverObstructionPolicy
                ),
            -- oo = outward obstruction = haOutwardObstructionPolicy
            'outward_obstruction',
                NAMED_STRUCT(
                    'harsh_event_name', 'haOutwardObstructionPolicy',
                    'enabled',
                    CASE
                        WHEN device_config.distracted_policy_detector_config.outward_obstruction_config.enabled IS NULL THEN FALSE
                        ELSE device_config.distracted_policy_detector_config.outward_obstruction_config.enabled
                    END,
                    'audio_alerts_enabled', aa.audio_alerts.haOutwardObstructionPolicy
                )
        ) AS harsh_event_settings,
        device_config.audio_alerts.audio_alert_configs
    FROM {database_silver_dev}.stg_devices_settings_latest_date cfg
    LEFT JOIN {database_silver_dev}.stg_devices_settings_audio_alerts aa ON cfg.`date` = aa.`date`
        AND cfg.org_id = aa.org_id
        AND cfg.device_id = aa.device_id
    WHERE cfg.`date` = '{DATEID}'
--endsql"""

dim_devices_settings_assets = build_assets_from_sql(
    name="dim_devices_settings",
    schema=dim_devices_settings_schema,
    description=build_table_description(
        table_desc="""This table provides a daily snapshot of a device's settings. Some questions that can be answered using this table: How have the device configuration(s) for an org changed over time? What share of an org's devices have a certain setting enabled?""",
        row_meaning="""Each row represents the first emitted setting from a device on the latest date with an emitted setting for a device.""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=dim_devices_settings_query,
    primary_keys=["date", "org_id", "device_id"],
    upstreams=[
        AssetKey(
            [
                databases["database_silver_dev"],
                "dq_stg_devices_settings_latest_date",
            ],
        ),
        AssetKey(
            [databases["database_silver_dev"], "dq_stg_devices_settings_audio_alerts"]
        ),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
    depends_on_past=True,  # Query will return misleading results if run when previous dates have not been materialized
)

dim_devices_settings_us = dim_devices_settings_assets[AWSRegions.US_WEST_2.value]
dim_devices_settings_eu = dim_devices_settings_assets[AWSRegions.EU_WEST_1.value]
dim_devices_settings_ca = dim_devices_settings_assets[AWSRegions.CA_CENTRAL_1.value]

dqs["dim_devices_settings"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_devices_settings",
        table="dim_devices_settings",
        primary_keys=["date", "org_id", "device_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

# Empty DQ Check
dqs["dim_devices_settings"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_devices_settings",
        table="dim_devices_settings",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

# Trend based DQ check
dqs["dim_devices_settings"].append(
    TrendDQCheck(
        name="dq_trend_dim_devices_settings",
        database=databases["database_gold_dev"],
        table="dim_devices_settings",
        blocking=False,
        tolerance=0.1,
    )
)

# Joinability check to dim_devices
dqs["dim_devices_settings"].append(
    JoinableDQCheck(
        name="dq_joinable_dim_devices_settings_to_dim_devices",
        database=databases["database_gold_dev"],
        database_2=databases["database_gold_dev"],
        input_asset_1="dim_devices_settings",
        input_asset_2="dim_devices",
        join_keys=[("org_id", "org_id"), ("device_id", "device_id")],
        blocking=True,
        null_right_table_rows_ratio=0.01,
    )
)

# Joinability check to dim_organizations
dqs["dim_devices_settings"].append(
    JoinableDQCheck(
        name="dq_joinable_dim_devices_settings_to_dim_organizations",
        database=databases["database_gold_dev"],
        database_2=databases["database_gold_dev"],
        input_asset_1="dim_devices_settings",
        input_asset_2="dim_organizations",
        join_keys=[("org_id", "org_id")],
        blocking=True,
        null_right_table_rows_ratio=0.01,
    )
)


dq_assets = dqs.generate()
