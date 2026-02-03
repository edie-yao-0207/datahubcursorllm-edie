from dagster import (
    AssetKey,
    BackfillPolicy,
    Backoff,
    DailyPartitionsDefinition,
    Jitter,
    RetryPolicy,
)

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    org_id_default_description,
)
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

pipeline_group_name = "dim_organizations_settings"
# end_offset=0 specifies that the final partition is the one ending at 00:00 on the current day.
# start_date is based on the earliest data available in upstream dependency dim_organizations
daily_partition_def = DailyPartitionsDefinition(start_date="2023-09-05", end_offset=0)

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

stg_organizations_speeding_settings_schema = [
    {"name": "shard_name", "type": "string", "nullable": False, "metadata": {}},
    {"name": "_filename", "type": "string", "nullable": True, "metadata": {}},
    {"name": "_op", "type": "string", "nullable": True, "metadata": {}},
    {
        "name": "_raw_speeding_settings_proto",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Speeding settings for org (raw value, don't use)"},
    },
    {"name": "_rowid", "type": "string", "nullable": True, "metadata": {}},
    {"name": "_timestamp", "type": "timestamp", "nullable": True, "metadata": {}},
    {
        "name": "created_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "When org was created"},
    },
    {
        "name": "created_by",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "ID of who org was created by"},
    },
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "speeding_settings_proto",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "max_speed_setting",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether max speed setting is enabled or not."
                                },
                            },
                            {
                                "name": "time_before_alert_ms",
                                "type": "long",
                                "nullable": True,
                                "metadata": {"comment": "Amount of time before alert."},
                            },
                            {
                                "name": "kmph_threshold",
                                "type": "float",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Max speed threshold (in Km/hr)"
                                },
                            },
                            {
                                "name": "in_cab_audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether audio alerts are enabled in cab or not for max speed."
                                },
                            },
                            {
                                "name": "send_to_safety_inbox",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether alert will be sent to safety inbox or not."
                                },
                            },
                            {
                                "name": "auto_add_to_coaching",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether alert needs coaching or not."
                                },
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {"comment": "Speeding settings for org"},
                },
                {
                    "name": "in_cab_severity_alert_setting",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether in cab severity alerts are enabled or not."
                                },
                            },
                            {
                                "name": "alert_at_severity_level",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Severity level at which to fire off alerts."
                                },
                            },
                            {
                                "name": "time_before_alert_ms",
                                "type": "long",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Time before alert is fired off."
                                },
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {"comment": "In cab severity alert settings for org."},
                },
                {
                    "name": "severity_settings_speed_over_limit_unit",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {"comment": "Unit to use for speeding limits"},
                },
                {
                    "name": "severity_settings",
                    "type": {
                        "type": "array",
                        "elementType": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "severity_level",
                                    "type": "integer",
                                    "nullable": True,
                                    "metadata": {"comment": "Severity level of alert."},
                                },
                                {
                                    "name": "speed_over_limit_threshold",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "Threshold value for speeding alert."
                                    },
                                },
                                {
                                    "name": "time_before_alert_ms",
                                    "type": "long",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "Time before alert is fired."
                                    },
                                },
                                {
                                    "name": "enabled",
                                    "type": "boolean",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "Whether alerts are enabled or not."
                                    },
                                },
                                {
                                    "name": "send_to_safety_inbox",
                                    "type": "boolean",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "Whether alerts will be sent to safety inbox or not."
                                    },
                                },
                                {
                                    "name": "auto_add_to_coaching",
                                    "type": "boolean",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "Whether alert needs coaching or not."
                                    },
                                },
                                {
                                    "name": "evidence_based_speeding_enabled",
                                    "type": "boolean",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "Whether evidence-based speeding is enabled for the organization or not."
                                    },
                                },
                            ],
                        },
                        "containsNull": True,
                    },
                    "nullable": True,
                    "metadata": {"comment": "Severity settings for org."},
                },
                {
                    "name": "csl_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not Contextual Speed Limit (CSL) is enabled for speeding events. When enabled, this feature uses GPS and map data to determine the current speed limit for the road segment, allowing for more accurate and context-aware speeding detection."
                    },
                },
            ],
        },
        "nullable": True,
        "metadata": {"comment": "Speeding settings"},
    },
    {
        "name": "uuid",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique ID for config."},
    },
    {"name": "org_rank", "type": "integer", "nullable": False, "metadata": {}},
    {"name": "org_count", "type": "long", "nullable": False, "metadata": {}},
]

stg_organizations_speeding_settings_query = """--sql
    -- Gets the most recent setting as of the partition run date.
    WITH order_settings AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY `date` DESC, created_at_ms DESC) AS org_rank,
            COUNT(created_at_ms) OVER (PARTITION BY org_id) AS org_count
        FROM safetydb_shards.org_safety_speeding_settings
        WHERE `date` <= '{DATEID}'
    )
    SELECT o.shard_name,
    o._filename,
    o._op,
    o._raw_speeding_settings_proto,
    o._rowid,
    o._timestamp,
    o.created_at_ms,
    o.created_by,
    o.date,
    o.org_id,
    named_struct(
      'max_speed_setting', o.speeding_settings_proto.max_speed_setting,
      'in_cab_severity_alert_setting', named_struct(
        'enabled', orgs.settings_proto.safety.in_cab_speed_limit_alert_settings.enabled,
        'alert_at_severity_level', case when orgs.settings_proto.safety.in_cab_speed_limit_alert_settings.kmph_over_limit_enabled
        then cast(orgs.settings_proto.safety.in_cab_speed_limit_alert_settings.kmph_over_limit_threshold as int)
        when orgs.settings_proto.safety.in_cab_speed_limit_alert_settings.percent_over_limit_enabled
        then cast(orgs.settings_proto.safety.in_cab_speed_limit_alert_settings.percent_over_limit_threshold as int)
        else null end,
        'time_before_alert_ms', orgs.settings_proto.safety.in_cab_speed_limit_alert_settings.time_before_alert_ms
      ),
      'severity_settings_speed_over_limit_unit', o.speeding_settings_proto.severity_settings_speed_over_limit_unit,
      'severity_settings', o.speeding_settings_proto.severity_settings,
      'csl_enabled', o.speeding_settings_proto.csl_enabled
    ) as speeding_settings_proto,
    o.uuid,
    o.org_rank,
    o.org_count
    FROM order_settings o
    join clouddb.organizations orgs
    on o.org_id = orgs.id
    WHERE org_rank = 1
--endsql"""

stg_organizations_speeding_settings_assets = build_assets_from_sql(
    name="stg_organizations_speeding_settings",
    schema=stg_organizations_speeding_settings_schema,
    description=build_table_description(
        table_desc="""A staging table containing the most recent speeding settings""",
        row_meaning="""The most recent speed settings for orgs""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=stg_organizations_speeding_settings_query,
    primary_keys=[
        "date",
        "org_id",
    ],
    upstreams=[
        AssetKey(["safetydb_shards", "organizations_speeding_settings"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.merge,
    partitions_def=daily_partition_def,
)

stg_organizations_speeding_settings_us = stg_organizations_speeding_settings_assets[
    AWSRegions.US_WEST_2.value
]
stg_organizations_speeding_settings_eu = stg_organizations_speeding_settings_assets[
    AWSRegions.EU_WEST_1.value
]
stg_organizations_speeding_settings_ca = stg_organizations_speeding_settings_assets[
    AWSRegions.CA_CENTRAL_1.value
]

dqs["stg_organizations_speeding_settings"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_organizations_speeding_settings",
        table="stg_organizations_speeding_settings",
        primary_keys=["date", "org_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)


stg_organizations_camera_settings_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "settings_hash_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Hash of settings values. This value will change whenever any of the settings values changes. As such, changes in settings can be identified by looking for changes in this value over time."
        },
    },
    {
        "name": "recording_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "cm_31_storage_hours_setting",
                    "type": "long",
                    "nullable": True,
                    "metadata": {"comment": "Storage hours setting for CM31 devices."},
                },
                {
                    "name": "cm_32_storage_hours_setting",
                    "type": "long",
                    "nullable": True,
                    "metadata": {"comment": "Storage hours setting for CM32 devices."},
                },
                {
                    "name": "high_res_storage_percent_setting",
                    "type": "long",
                    "nullable": True,
                    "metadata": {
                        "comment": "High res storage percent setting for camera."
                    },
                },
                {
                    "name": "parking_mode",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "duration_seconds",
                                "type": "long",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Seconds needed to be considered in parking mode."
                                },
                            }
                        ],
                    },
                    "nullable": True,
                    "metadata": {"comment": "Parking settings"},
                },
                {
                    "name": "ahd_1_storage_hours_setting",
                    "type": "long",
                    "nullable": True,
                    "metadata": {"comment": "Storage hours setting for AHD 1"},
                },
                {
                    "name": "cm_33_storage_hours_setting",
                    "type": "long",
                    "nullable": True,
                    "metadata": {"comment": "Storage hours setting for CM33 devices."},
                },
                {
                    "name": "cm_34_storage_hours_setting",
                    "type": "long",
                    "nullable": True,
                    "metadata": {"comment": "Storage hours setting for CM34 devices."},
                },
                {
                    "name": "has_2k_resolution_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether camera has 2k resolution enabled or not."
                    },
                },
            ],
        },
        "nullable": True,
        "metadata": {"comment": "Recording settings for camera."},
    },
    {
        "name": "audio_recording_enabled",
        "type": "integer",
        "nullable": True,
        "metadata": {"comment": "Whether audio recording is enabled or not."},
    },
    {
        "name": "camera_id_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {"comment": "Whether camera is enabled or not."},
                },
                {
                    "name": "confirmation_mode_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether confirmation mode is enabled for the camera or not."
                    },
                },
                {
                    "name": "autoassignment_groups_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether autoassignment groups for the camera are enabled or not."
                    },
                },
            ],
        },
        "nullable": False,
        "metadata": {"comment": "Camera settings."},
    },
    {
        "name": "livestream_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {"comment": "Whether livestreaming is enabled or not."},
                },
                {
                    "name": "outward_camera_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether outward camera is enabled or not."
                    },
                },
                {
                    "name": "inward_camera_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {"comment": "Whether inward camera is enabled or not."},
                },
                {
                    "name": "helicopter_view_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether helicopter view is enabled or not."
                    },
                },
                {
                    "name": "allowance_secs",
                    "type": "long",
                    "nullable": True,
                    "metadata": {"comment": "Seconds left for livestreaming."},
                },
                {
                    "name": "outward_camera_settings",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "bits_per_second",
                                "type": "long",
                                "nullable": True,
                                "metadata": {"comment": "BPS of outward camera."},
                            },
                            {
                                "name": "frames_per_second",
                                "type": "long",
                                "nullable": True,
                                "metadata": {"comment": "FPS of outward camera."},
                            },
                            {
                                "name": "resolution",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Resolution of outward camera."
                                },
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {
                        "comment": "Outward camera settings for livestreaming."
                    },
                },
                {
                    "name": "inward_camera_settings",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "bits_per_second",
                                "type": "long",
                                "nullable": True,
                                "metadata": {"comment": "BPS for inward camera."},
                            },
                            {
                                "name": "frames_per_second",
                                "type": "long",
                                "nullable": True,
                                "metadata": {"comment": "FPS for inward camera."},
                            },
                            {
                                "name": "resolution",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {"camera": "Resolution for inward camera."},
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {"comment": "Livestream settings for inward camera."},
                },
                {
                    "name": "max_session_duration_seconds",
                    "type": "long",
                    "nullable": True,
                    "metadata": {"comment": "Max duration for livestreaming session."},
                },
                {
                    "name": "audio_stream_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {"comment": "Whether audio is enabled for livestream."},
                },
            ],
        },
        "nullable": True,
        "metadata": {"comment": "Livestream settings for camera"},
    },
]

stg_organizations_camera_settings_query = """--sql
WITH org_camera_settings AS (
    SELECT
        orgs.`date`,
        orgs.id AS org_id,
        orgs.settings_proto.safety.recording_settings,
        CAST(orgs.audio_recording_enabled AS INT) AS audio_recording_enabled,
        NAMED_STRUCT(
            'enabled', orgs.settings_proto.face_detection_enabled,
            'confirmation_mode_enabled', orgs.settings_proto.safety.camera_id_settings.confirmation_mode_enabled,
            'autoassignment_groups_enabled', orgs.settings_proto.safety.camera_id_settings.autoassignment_groups_enabled
        ) AS camera_id_settings,
        orgs.settings_proto.safety.livestream_settings
    FROM datamodel_core_bronze.raw_clouddb_organizations orgs
    WHERE `date` = '{DATEID}'
)
SELECT
    `date`,
    org_id,
    -- All settings columns from the CTE above should be hashed here to create a unique identifier for the settings.
    XXHASH64(
        org_id,
        recording_settings,
        audio_recording_enabled,
        camera_id_settings,
        livestream_settings
    ) AS settings_hash_id,
    recording_settings,
    audio_recording_enabled,
    camera_id_settings,
    livestream_settings
FROM org_camera_settings
--endsql"""

stg_organizations_camera_settings_assets = build_assets_from_sql(
    name="stg_organizations_camera_settings",
    schema=stg_organizations_camera_settings_schema,
    description=build_table_description(
        table_desc="""This table provides a daily snapshot of an organization's camera settings. As such, it can be used to track changes to an organization's settings over time. More general information about an organization can be found in dim_organizations. The settings are sourced from clouddb.organizations. This table was built to support the work described in the project proposal here: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4411902/Project+Proposal+-+Usage+Visibility+of+Camera+Settings.""",
        row_meaning="""An organization's settings on a given date.""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=stg_organizations_camera_settings_query,
    primary_keys=[
        "date",
        "org_id",
    ],
    upstreams=[
        AssetKey(["datamodel_core_bronze", "raw_clouddb_organizations"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.merge,
    partitions_def=daily_partition_def,
)

stg_organizations_camera_settings_us = stg_organizations_camera_settings_assets[
    AWSRegions.US_WEST_2.value
]
stg_organizations_camera_settings_eu = stg_organizations_camera_settings_assets[
    AWSRegions.EU_WEST_1.value
]
stg_organizations_camera_settings_ca = stg_organizations_camera_settings_assets[
    AWSRegions.CA_CENTRAL_1.value
]

dqs["stg_organizations_camera_settings"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_organizations_camera_settings",
        table="stg_organizations_camera_settings",
        primary_keys=["date", "org_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)


dim_organizations_settings_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The date the organization's settings were logged (yyyy-mm-dd)."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "camera_id_enabled",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether or not face detection using the camera is enabled"
        },
    },
    {
        "name": "rolling_stop_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not rolling stop events are enabled (default: True)"
                    },
                },
                {
                    "name": "threshold_milliknots",
                    "type": "long",
                    "nullable": True,
                    "metadata": {
                        "comment": "The threshold speed in milliknots for rolling stop events (default value: 8690)"
                    },
                },
            ],
        },
        "nullable": False,
        "metadata": {
            "comment": "Settings for rolling stop events. Snapshotted based on datamodel_core_bronze.raw_clouddb_organizations"
        },
    },
    {
        "name": "forward_collision_warning_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not forward collision warning events are enabled (default: False)"
                    },
                },
                {
                    "name": "audio_alerts_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not audio alerts are enabled for forward collision warning events (default: False)"
                    },
                },
                {
                    "name": "sensitivity_level",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {
                        "comment": "The sensitivity level for forward collision warning events. Value depends on device.\n"
                        "0: FORWARD_COLLISION_DEFAULT\n"
                        "1: FORWARD_COLLISION_LOW\n"
                        "2: FORWARD_COLLISION_MEDIUM\n"
                        "3: FORWARD_COLLISION_HIGH"
                    },
                },
            ],
        },
        "nullable": False,
        "metadata": {
            "comment": "Settings for forward collision warning events. Snapshotted based on datamodel_core_bronze.raw_clouddb_organizations"
        },
    },
    {
        "name": "distracted_driving_detection_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not distracted driving detection events are enabled (default: True)"
                    },
                },
                {
                    "name": "audio_alerts_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not audio alerts are enabled for distracted driving detection events (default: False)"
                    },
                },
                {
                    "name": "sensitivity_level",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {
                        "comment": "The sensitivity level for distracted driving detection events.\n"
                        "0: DISTRACTED_DRIVING_DEFAULT (defaults to LOW)\n"
                        "1: DISTRACTED_DRIVING_LOW\n"
                        "2: DISTRACTED_DRIVING_MEDIUM\n"
                        "3: DISTRACTED_DRIVING_HIGH"
                    },
                },
                {
                    "name": "minimum_speed_enum",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {
                        "comment": "The minimum speed for distracted driving detection events.\n"
                        "0: DISTRACTED_DRIVING_MIN_SPEED_DEFAULT (25 MPH)\n"
                        "1: DISTRACTED_DRIVING_MIN_SPEED_SETTING_1 (0 MPH)\n"
                        "2: DISTRACTED_DRIVING_MIN_SPEED_SETTING_2 (15 KMPH)\n"
                        "3: DISTRACTED_DRIVING_MIN_SPEED_SETTING_3 (25 MPH)\n"
                        "4: DISTRACTED_DRIVING_MIN_SPEED_SETTING_4 (55 KMPH)"
                    },
                },
                {
                    "name": "minimum_duration_enum",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {
                        "comment": "The minimum duration for distracted driving detection events.\n"
                        "0: DISTRACTED_DRIVING_MIN_DURATION_DEFAULT (9000 milliseconds)\n"
                        "1: DISTRACTED_DRIVING_MIN_DURATION_LOW (7000 milliseconds)\n"
                        "2: DISTRACTED_DRIVING_MIN_DURATION_MEDIUM (9000 milliseconds)\n"
                        "3: DISTRACTED_DRIVING_MIN_DURATION_HIGH (11000 milliseconds)"
                    },
                },
            ],
        },
        "nullable": False,
        "metadata": {
            "comment": "Settings for distracted driving detection events. Snapshotted based on datamodel_core_bronze.raw_clouddb_organizations"
        },
    },
    {
        "name": "following_distance_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not following distance events are enabled (default: True)"
                    },
                },
                {
                    "name": "audio_alerts_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not audio alerts are enabled for following distance events (default: False)"
                    },
                },
                {
                    "name": "minimum_speed_enum",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {
                        "comment": "The minimum speed for following distance events.\n"
                        "0: FOLLOWING_DISTANCE_MIN_SPEED_DEFAULT (55 MPH)\n"
                        "1: FOLLOWING_DISTANCE_MIN_SPEED_LOW (35 MPH)\n"
                        "2: FOLLOWING_DISTANCE_MIN_SPEED_MEDIUM (45 MPH)\n"
                        "3: FOLLOWING_DISTANCE_MIN_SPEED_HIGH (55 MPH)"
                    },
                },
                {
                    "name": "minimum_duration_enum",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {
                        "comment": "The minimum duration for following distance events.\n"
                        "0: FOLLOWING_DISTANCE_MIN_DURATION_DEFAULT (60 seconds)\n"
                        "1: FOLLOWING_DISTANCE_MIN_DURATION_LOW (15 seconds)\n"
                        "2: FOLLOWING_DISTANCE_MIN_DURATION_MEDIUM (30 seconds)\n"
                        "3: FOLLOWING_DISTANCE_MIN_DURATION_HIGH (60 seconds)"
                    },
                },
                {
                    "name": "minimum_following_distance_seconds",
                    "type": "float",
                    "nullable": True,
                    "metadata": {
                        "comment": "The minimum following distance in seconds for following distance events (default value: 0.5). Acceptable values are < 0.5 seconds, < 1 seconds, and < 1.5 seconds"
                    },
                },
            ],
        },
        "nullable": False,
        "metadata": {
            "comment": "Settings for following distance events. Snapshotted based on datamodel_core_bronze.raw_clouddb_organizations"
        },
    },
    {
        "name": "policy_violation_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not policy violation events are enabled (default: False)"
                    },
                },
                {
                    "name": "audio_alerts_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not audio alerts are enabled for policy violation events (default: False)"
                    },
                },
                {
                    "name": "minimum_speed",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {
                        "comment": "The minimum speed for policy violation events in miles/h\n"
                        "0: MINIMUM_SPEED_DEFAULT (which defaults to 25 MPH)\n"
                        "1: MINIMUM_SPEED_5_MPH\n"
                        "2: MINIMUM_SPEED_10_MPH\n"
                        "3: MINIMUM_SPEED_25_MPH\n"
                        "4: MINIMUM_SPEED_35_MPH"
                    },
                },
                {
                    "name": "seatbelt_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not seatbelt events are enabled (default: False)"
                    },
                },
                {
                    "name": "seatbelt_audio_alerts_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not audio alerts are enabled for seatbelt events (default: False)"
                    },
                },
                {
                    "name": "inward_obstruction_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not inward obstruction events are enabled (default: False)"
                    },
                },
                {
                    "name": "inward_obstruction_audio_alerts_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not audio alerts are enabled for inward obstruction events (default: False)"
                    },
                },
            ],
        },
        "nullable": False,
        "metadata": {
            "comment": "Settings for policy violation events; includes seatbelt and inward obstruction. Snapshotted based on datamodel_core_bronze.raw_clouddb_organizations"
        },
    },
    {
        "name": "tile_rolling_stop_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not tile-based rolling stop events are enabled (default: True)"
                    },
                },
                {
                    "name": "audio_alerts_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not audio alerts are enabled for tile-based rolling stop events (default: False)"
                    },
                },
                {
                    "name": "stop_threshold_kmph",
                    "type": "float",
                    "nullable": True,
                    "metadata": {
                        "comment": "The stop threshold in km/h for tile-based rolling stop events (default value: 0)"
                    },
                },
            ],
        },
        "nullable": False,
        "metadata": {
            "comment": "Settings for tile-based rolling stop events. Snapshotted based on datamodel_core_bronze.raw_clouddb_organizations"
        },
    },
    {
        "name": "lane_departure_warning_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not lane departure warning events are enabled (default: False)"
                    },
                },
                {
                    "name": "audio_alerts_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not audio alerts are enabled for lane departure warning events (default: False)"
                    },
                },
            ],
        },
        "nullable": False,
        "metadata": {
            "comment": "Settings for lane departure warning events. Snapshotted based on datamodel_core_bronze.raw_clouddb_organizations"
        },
    },
    {
        "name": "drowsiness_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not drowsiness events are enabled (default: False)"
                    },
                },
                {
                    "name": "audio_alerts_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not audio alerts are enabled for drowsiness events (default: False)"
                    },
                },
                {
                    "name": "sensitivity_level",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {
                        "comment": "The sensitivity level for drowsiness events (default: EXTREMELY_DROWSY).\n"
                        "0: SEVERITY_UNKNOWN\n"
                        "1: SEVERITY_NOT_DROWSY\n"
                        "2: SEVERITY_SLIGHTLY_DROWSY\n"
                        "3: SEVERITY_MODERATELY_DROWSY\n"
                        "4: SEVERITY_VERY_DROWSY\n"
                        "5: SEVERITY_EXTREMELY_DROWSY"
                    },
                },
            ],
        },
        "nullable": False,
        "metadata": {
            "comment": "Settings for drowsiness events. Snapshotted based on datamodel_core_bronze.raw_clouddb_organizations"
        },
    },
    {
        "name": "mobile_usage_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not mobile usage events are enabled (default: False)"
                    },
                },
                {
                    "name": "audio_alerts_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not audio alerts are enabled for mobile usage events (default: False)"
                    },
                },
                {
                    "name": "minimum_speed_enum",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {
                        "comment": "The minimum speed for mobile usage events.\n"
                        "0: DEFAULT (defaults to 25 MPH)\n"
                        "1: 5 MPH\n"
                        "2: 10 MPH\n"
                        "3: 25 MPH\n"
                        "4: 35 MPH\n"
                        "5: 45 MPH\n"
                        "6: 55 MPH"
                    },
                },
                {
                    "name": "severity_threshold_enum",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {
                        "comment": "The severity threshold for mobile usage events (default depends on device).\n"
                        "0: Invalid\n"
                        "1: Default\n"
                        "2: Low\n"
                        "3: Medium\n"
                        "4: High"
                    },
                },
                {
                    "name": "audio_alert_promotion_settings",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for mobile usage events (default: False)"
                                },
                            },
                            {
                                "name": "alert_count_threshold",
                                "type": "long",
                                "nullable": True,
                                "metadata": {
                                    "comment": "The alert count threshold for mobile usage events (default value: 2)"
                                },
                            },
                            {
                                "name": "duration_ms",
                                "type": "long",
                                "nullable": True,
                                "metadata": {
                                    "comment": "The duration in milliseconds for mobile usage events (default value: 43200000)"
                                },
                            },
                            {
                                "name": "per_trip_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not per-trip evaluation is enabled for mobile usage events"
                                },
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {
                        "comment": "The audio alert promotion settings for mobile usage events"
                    },
                },
            ],
        },
        "nullable": True,
        "metadata": {
            "comment": "Settings for mobile usage events. Latest based on safetydb_shards.organization_settings"
        },
    },
    {
        "name": "speeding_settings",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "max_speed_setting",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not the max speed setting is enabled (default: True)"
                                },
                            },
                            {
                                "name": "time_before_alert_ms",
                                "type": "long",
                                "nullable": True,
                                "metadata": {
                                    "comment": "The time before alert in milliseconds for speeding events (default value: 60000)"
                                },
                            },
                            {
                                "name": "kmph_threshold",
                                "type": "float",
                                "nullable": True,
                                "metadata": {
                                    "comment": "The km/h threshold for speeding events"
                                },
                            },
                            {
                                "name": "in_cab_audio_alerts_enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not audio alerts are enabled for speeding events (default: False)"
                                },
                            },
                            {
                                "name": "send_to_safety_inbox",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not to send speeding events to the safety inbox (default: False)"
                                },
                            },
                            {
                                "name": "auto_add_to_coaching",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not to automatically add speeding events to coaching (default: False)"
                                },
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {
                        "comment": "The max speed setting for speeding events"
                    },
                },
                {
                    "name": "in_cab_severity_alert_setting",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "enabled",
                                "type": "boolean",
                                "nullable": True,
                                "metadata": {
                                    "comment": "Whether or not the in-cab severity alert setting is enabled (default: False)"
                                },
                            },
                            {
                                "name": "alert_at_severity_level",
                                "type": "integer",
                                "nullable": True,
                                "metadata": {
                                    "comment": "The alert at severity level for speeding events (the KMPH or percent severity level)"
                                },
                            },
                            {
                                "name": "time_before_alert_ms",
                                "type": "long",
                                "nullable": True,
                                "metadata": {
                                    "comment": "The time before alert in milliseconds for speeding events (default value: 60000)"
                                },
                            },
                        ],
                    },
                    "nullable": True,
                    "metadata": {
                        "comment": "The in-cab severity alert setting for speeding events"
                    },
                },
                {
                    "name": "severity_settings_speed_over_limit_unit",
                    "type": "integer",
                    "nullable": True,
                    "metadata": {
                        "comment": "The unit used for speeding events\n"
                        "0: Invalid unit\n"
                        "1: Percentage\n"
                        "2: Miles per hour\n"
                        "3: Kilomters per hour \n"
                        "4: Milliknots per hour"
                    },
                },
                {
                    "name": "severity_settings",
                    "type": {
                        "type": "array",
                        "elementType": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "severity_level",
                                    "type": "integer",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "The severity level for speeding events\n"
                                        "0: Invalid severity\n"
                                        "1: Not speeding\n"
                                        "2: Light\n"
                                        "3: Moderate\n"
                                        "4: Heavy\n"
                                        "5: Severe"
                                    },
                                },
                                {
                                    "name": "speed_over_limit_threshold",
                                    "type": "float",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "The speed over limit threshold for speeding events. Default values are as follows:\n"
                                        "Light: 1 mph/1 kmph/1%\n"
                                        "Moderate: 5 mph/10 kmph/10%\n"
                                        "Heavy: 10 mph/20 kmph/20%\n"
                                        "Severe: 15 mph/30 kpmh/30%"
                                    },
                                },
                                {
                                    "name": "time_before_alert_ms",
                                    "type": "long",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "The time before alert in milliseconds for speeding events (default value: 60000)"
                                    },
                                },
                                {
                                    "name": "enabled",
                                    "type": "boolean",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "Whether or not the speed over limit threshold is enabled for speeding events (default: True)"
                                    },
                                },
                                {
                                    "name": "send_to_safety_inbox",
                                    "type": "boolean",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "Whether or not to send speeding events to the safety inbox for speeding events (default: False)"
                                    },
                                },
                                {
                                    "name": "auto_add_to_coaching",
                                    "type": "boolean",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "Whether or not to automatically add speeding events to coaching for speeding events (default: False)"
                                    },
                                },
                                {
                                    "name": "evidence_based_speeding_enabled",
                                    "type": "boolean",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "Whether evidence-based speeding is enabled for the organization or not."
                                    },
                                },
                            ],
                        },
                        "containsNull": True,
                    },
                    "nullable": True,
                    "metadata": {
                        "comment": "The severity settings for speeding events"
                    },
                },
                {
                    "name": "csl_enabled",
                    "type": "boolean",
                    "nullable": True,
                    "metadata": {
                        "comment": "Whether or not Contextual Speed Limit (CSL) is enabled for speeding events. When enabled, this feature uses GPS and map data to determine the current speed limit for the road segment, allowing for more accurate and context-aware speeding detection (default: False)."
                    },
                },
            ],
        },
        "nullable": True,
        "metadata": {
            "comment": "Settings for speeding events. Snapshotted based on datamodel_core_silver.stg_organizations_speeding_settings"
        },
    },
]

dim_organizations_settings_query = """--sql
    -- Organization safety setting are in the process of being migrated from clouddb to safetydb.
    -- As such, the source for some settings will be clouddb.organizations while the source for
    -- others will be safetydb.organization_settings, depending on migration progress.
    -- Note: settings in safetydb are organized in structs by event type
    WITH latest_speeding_settings AS (
        SELECT org_id,
        speeding_settings_proto,
        ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY date DESC, created_at_ms DESC) AS rn
        FROM {database_silver_dev}.stg_organizations_speeding_settings
    )
    SELECT
        orgs.`date`,
        orgs.id AS org_id,
        orgs.settings_proto.face_detection_enabled AS camera_id_enabled,
        -- rolling stop (no alert?)
        NAMED_STRUCT(
        'enabled', CAST(orgs.rolling_stop_enabled AS BOOLEAN),
        'threshold_milliknots', orgs.rolling_stop_threshold_milliknots
        ) AS rolling_stop_settings,
        -- forward collision warning (fcw)
        NAMED_STRUCT(
        'enabled', orgs.settings_proto.forward_collision_warning_enabled,
        'audio_alerts_enabled', orgs.settings_proto.forward_collision_warning_audio_alerts_enabled,
        'sensitivity_level', orgs.settings_proto.forward_collision_sensitivity_level
        ) AS forward_collision_warning_settings,
        -- distracted driving (ddd)
        NAMED_STRUCT(
        'enabled', orgs.settings_proto.distracted_driving_detection_enabled,
        'audio_alerts_enabled', orgs.settings_proto.distracted_driving_audio_alerts_enabled,
        'sensitivity_level', orgs.settings_proto.distracted_driving_sensitivity_level,
        'minimum_speed_enum', orgs.settings_proto.safety.distracted_driving_detection_settings.minimum_speed_enum,
        'minimum_duration_enum', orgs.settings_proto.safety.distracted_driving_detection_settings.minimum_duration_enum
        ) AS distracted_driving_detection_settings,
        -- following distance (fd)
        NAMED_STRUCT(
        'enabled', orgs.settings_proto.following_distance_enabled,
        'audio_alerts_enabled', orgs.settings_proto.following_distance_audio_alerts_enabled,
        'minimum_speed_enum', orgs.settings_proto.safety.following_distance_settings.minimum_speed_enum,
        'minimum_duration_enum', orgs.settings_proto.safety.following_distance_settings.minimum_duration_enum,
        'minimum_following_distance_seconds', orgs.settings_proto.safety.following_distance_settings.minimum_following_distance_seconds
        ) AS following_distance_settings,
        -- Some event types are grouped under the policy violation heading
        NAMED_STRUCT(
        'enabled', orgs.settings_proto.safety.policy_violation_settings.enabled,
        'audio_alerts_enabled', orgs.settings_proto.safety.policy_violation_settings.audio_alert_settings.enabled,
        'minimum_speed', settings_proto.safety.policy_violation_settings.minimum_speed,
        'seatbelt_enabled', orgs.settings_proto.safety.policy_violation_settings.seatbelt_enabled,
        'seatbelt_audio_alerts_enabled', orgs.settings_proto.safety.policy_violation_settings.audio_alert_settings.seatbelt_enabled,
        'inward_obstruction_enabled', orgs.settings_proto.safety.policy_violation_settings.camera_obstruction_enabled,
        'inward_obstruction_audio_alerts_enabled', orgs.settings_proto.safety.policy_violation_settings.audio_alert_settings.camera_obstruction_enabled
        ) AS policy_violation_settings,
        -- tile-based rolling stop
        NAMED_STRUCT(
        'enabled', orgs.settings_proto.safety.in_cab_stop_sign_violation_alert_settings.enabled,
        'audio_alerts_enabled', settings_proto.safety.in_cab_stop_sign_violation_alert_settings.enable_audio_alerts,
        'stop_threshold_kmph', settings_proto.safety.in_cab_stop_sign_violation_alert_settings.stop_threshold_kmph
        ) AS tile_rolling_stop_settings,
        -- lane-departure warning (ldw)
        NAMED_STRUCT(
        'enabled', orgs.settings_proto.safety.lane_departure_warning_settings.lane_departure_warning_enabled,
        'audio_alerts_enabled', orgs.settings_proto.safety.lane_departure_warning_settings.lane_departure_warning_audio_alerts_enabled
        ) AS lane_departure_warning_settings,
        -- drowsiness (d3)
        NAMED_STRUCT(
        'enabled', orgs.settings_proto.safety.drowsiness_detection_settings.drowsiness_detection_enabled,
        'audio_alerts_enabled', orgs.settings_proto.safety.drowsiness_detection_settings.drowsiness_detection_audio_alerts_enabled,
        'sensitivity_level', orgs.settings_proto.safety.drowsiness_detection_settings.drowsiness_detection_sensitivity_level
        ) AS drowsiness_settings,
        -- mobile usage MTL (hac) (haPhonePolicy)
        -- The proto construction in safetydb.organization_settings is in the format of this table.
        safety_orgs.organization_setting.mobile_usage_settings AS mobile_usage_settings,
        -- speeding settings
        speeding_orgs.speeding_settings_proto AS speeding_settings
    FROM datamodel_core_bronze.raw_clouddb_organizations orgs
    LEFT JOIN safetydb_shards.organization_settings safety_orgs ON orgs.id = safety_orgs.org_id
    LEFT JOIN latest_speeding_settings speeding_orgs ON orgs.id = speeding_orgs.org_id
        AND speeding_orgs.rn = 1
    WHERE orgs.`date` = '{DATEID}'
--endsql"""

dim_organizations_settings_assets = build_assets_from_sql(
    name="dim_organizations_settings",
    schema=dim_organizations_settings_schema,
    description=build_table_description(
        table_desc="""This table provides a daily snapshot of an organization's settings. As such, it can be used to track changes to an organization's settings over time. More general information about an organization can be found in dim_organizations. The settings are sourced from clouddb.organizations, safetydb.organization_settings, and safetydb.org_safety_speeding_settings.""",
        row_meaning="""An organization's settings on a given date.""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=dim_organizations_settings_query,
    primary_keys=["date", "org_id"],
    upstreams=[
        AssetKey(
            [databases["database_silver_dev"], "dq_stg_organizations_speeding_settings"]
        ),
        AssetKey([Database.DATAMODEL_CORE_BRONZE, "raw_clouddb_organizations"]),
        AssetKey(["safetydb_shards", "organization_settings"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.merge,
    partitions_def=daily_partition_def,
)

dim_organizations_settings_us = dim_organizations_settings_assets[
    AWSRegions.US_WEST_2.value
]
dim_organizations_settings_eu = dim_organizations_settings_assets[
    AWSRegions.EU_WEST_1.value
]
dim_organizations_settings_ca = dim_organizations_settings_assets[
    AWSRegions.CA_CENTRAL_1.value
]

dqs["dim_organizations_settings"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_organizations_settings",
        table="dim_organizations_settings",
        primary_keys=["date", "org_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_organizations_settings"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_organizations_settings",
        table="dim_organizations_settings",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_organizations_settings"].append(
    TrendDQCheck(
        name="dq_trend_dim_organizations_settings",
        database=databases["database_gold_dev"],
        table="dim_organizations_settings",
        blocking=True,
        tolerance=0.04,
    )
)

dqs["dim_organizations_settings"].append(
    JoinableDQCheck(
        name="dq_joinable_dim_organizations_settings_to_dim_organizations",
        database=databases["database_gold_dev"],
        database_2=databases["database_gold_dev"],
        input_asset_1="dim_organizations_settings",
        input_asset_2="dim_organizations",
        join_keys=[("org_id", "org_id")],
        blocking=True,
        null_right_table_rows_ratio=0.01,
    )
)

dq_assets = dqs.generate()
