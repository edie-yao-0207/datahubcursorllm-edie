from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_all_regions,
    partition_key_ranges_from_context,
)
from pyspark.sql import SparkSession

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE,
        },
    },
    {
        "name": "vg_device_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Device ID of VG device"},
    },
    {
        "name": "vg_serial",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Serial of VG device"},
    },
    {
        "name": "vg_product_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Product ID of VG device"},
    },
    {
        "name": "vg_product_name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Product name of VG device"},
    },
    {
        "name": "cm_device_id",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Device ID of corresponding CM device"},
    },
    {
        "name": "cm_serial",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Serial of corresponding CM device"},
    },
    {
        "name": "cm_product_id",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Product ID of corresponding CM device"},
    },
    {
        "name": "cm_type",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Type of CM device (inward, outward)"},
    },
    {
        "name": "cm_product_name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Product name of corresponding CM device"},
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
        "name": "vg_device_name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Device name of VG device"},
    },
    {
        "name": "device_policy_violation_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether policy violations are enabled on the device or not (from devices table)"
        },
    },
    {
        "name": "device_policy_violation_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for policy violations on the device or not (from devices table)"
        },
    },
    {
        "name": "device_phone_usage_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether phone usage notifications are enabled on the device or not (from devices table)"
        },
    },
    {
        "name": "device_rolling_stop_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether rolling stop notifications are enabled on the device or not (from devices table)"
        },
    },
    {
        "name": "device_rolling_stop_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether rolling stop audio alerts are enabled on the device or not (from devices table)"
        },
    },
    {
        "name": "device_rolling_stop_threshold_kmph",
        "type": "float",
        "nullable": False,
        "metadata": {
            "comment": "Threshold speed (in km/h) for rolling stop notifications on the device (from devices table)"
        },
    },
    {
        "name": "device_drowsiness_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether drowsiness detection is enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_drowsiness_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether drowsiness detection audio alerts are enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_drowsiness_detection_sensitivity_level",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Sensitivity level for drowsiness detection on the device (from devices table)"
        },
    },
    {
        "name": "device_in_cab_speed_limit_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab speed audio alerts are enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_distracted_driving_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether distracted driving notifications are enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_distracted_driving_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether distracted driving audio alerts are enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_forward_collision_warning_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether forward collision warning notifications are enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_forward_collision_warning_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether forward collision warning audio alerts are enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_forward_collision_sensitivity_level",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Sensitivity level to use for FCW alerts"
        },
    },
    {
        "name": "device_following_distance_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether following distance notifications are enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_following_distance_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether following distance audio alerts are enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_seatbelt_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether seatbelt alerts are enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_seatbelt_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether seatbelt audio alerts are enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_seatbelt_minimum_speed",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed to consider for seatbelt alerts (from devices table)"
        },
    },
    {
        "name": "device_inward_obstruction_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether inward obstruction alerts are enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_inward_obstruction_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether inward obstruction audio alerts are enabled for the device or not (from devices table)"
        },
    },
    {
        "name": "device_inward_obstruction_minimum_speed",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed to consider for inward obstruction alerts (from devices table)"
        },
    },
    {
        "name": "safety_setting_updated_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {"comment": "Time at which safety settings were last updated"},
    },
    {
        "name": "safety_setting_created_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {"comment": "Time at which safety settings were initially created"},
    },
    {
        "name": "mobile_usage_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether mobile usage settings are enabled for the device or not (from safety device settings table)"
        },
    },
    {
        "name": "mobile_usage_audio_alerts_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for mobile usage violations or not (from safety device settings table)"
        },
    },
    {
        "name": "mobile_usage_audio_alert_promotion_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alert promotions are enabled for mobile usage violations or not (from safety device settings table)"
        },
    },
    {
        "name": "distracted_driving_audio_alert_promotion_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alert promotion is enabled or not for distracted driving violations (from safety device settings table)"
        },
    },
    {
        "name": "following_distance_audio_alert_promotion_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alert promotion is enabled for following distance violations or not (from device safety settings table)"
        },
    },
    {
        "name": "eating_drinking_audio_alert_promotion_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alert promotion is enabled for eating/drinking violations or not (from device safety settings table)"
        },
    },
    {
        "name": "lane_departure_warning_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether lane departure warnings are enabled or not (from device safety settings table)"
        },
    },
    {
        "name": "lane_departure_warning_audio_alerts_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for lane departure warning violations or not (from device safety settings table)"
        },
    },
    {
        "name": "lane_departure_warning_audio_alert_promotion_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alert promotion is enabled for lane departure warning violations or not (from safety device settings table)"
        },
    },
    {
        "name": "ml_mode",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether ML mode settings are enabled or not (from device safety settings table)"
        },
    },
    {
        "name": "seatbelt_audio_alert_promotion_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alert promotion is enabled for seatbelt violations or not (from safety device settings table)"
        },
    },
    {
        "name": "inward_obstruction_audio_alert_promotion_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alert promotion is enabled for inward obstruction violations or not (from safety device settings table)"
        },
    },
    {
        "name": "external_camera",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "External recording camera (from safety device settings table)"
        },
    },
    {
        "name": "primary_camera",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Primary recording camera (from safety device settings table)"
        },
    },
    {
        "name": "driver_facing_camera",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Driver-facing camera for recording (from safety device settings table)"
        },
    },
    {
        "name": "device_csl_enabled",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether Commercial Speed Limits are enabled for the device or not"
        },
    },
    {
        "name": "device_severity_settings_speed_over_limit_unit",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Unit of measure for tracking severe speeding alerts (MPH, milliknots per hour, percentage)"
        },
    },
    {
        "name": "device_max_speed_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "Whether max speed is enabled for the device or not"},
    },
    {
        "name": "device_max_speed_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether max speed alerts are sent to safety inbox or not"
        },
    },
    {
        "name": "device_max_speed_auto_add_to_coaching",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether max speed alerts are added to coaching or not"
        },
    },
    {
        "name": "device_in_cab_severity_alert_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab severity alerts are enabled for the device or not"
        },
    },
    {
        "name": "device_in_cab_speed_limit_kmph_over_limit_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab severity alerts are enabled for the device or not based on KMPH"
        },
    },
    {
        "name": "device_in_cab_speed_limit_kmph_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {
            "comment": "Threshold in KMPH for in-cab severity alerts"
        },
    },
    {
        "name": "device_in_cab_speed_limit_percent_over_limit_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab severity alerts are enabled for the device or not based on percentage"
        },
    },
    {
        "name": "device_in_cab_speed_limit_percent_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {
            "comment": "Threshold in percentage for in-cab severity alerts"
        },
    },
    {
        "name": "device_in_cab_speed_limit_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Time before alert (in milliseconds) for speeding severity in-cab alerts"
        },
    },
    {
        "name": "device_in_cab_severity_alert_alert_at_severity_level",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Level at which to raise in-cab alerts"},
    },
    {
        "name": "device_in_cab_severity_alert_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before in-cab alerts are raised"},
    },
    {
        "name": "device_light_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {"comment": "Threshold for light speeding alerts"},
    },
    {
        "name": "device_light_speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before alert for light speeding"},
    },
    {
        "name": "device_light_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether light speeding alerts are enabled for the device or not"
        },
    },
    {
        "name": "device_light_speeding_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether light speeding alerts are sent to Safety Inbox or not"
        },
    },
    {
        "name": "device_light_speeding_auto_add_to_coaching",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether light speeding alerts are added to coaching or not"
        },
    },
    {
        "name": "device_moderate_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {"comment": "Speed threshold for moderate speeding alerts"},
    },
    {
        "name": "device_moderate_speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Time before which moderate speeding alerts are raised"
        },
    },
    {
        "name": "device_moderate_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether moderate speeding settings are enabled for the device or not"
        },
    },
    {
        "name": "device_moderate_speeding_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether moderate speeding alerts are sent to Safety Inbox or not"
        },
    },
    {
        "name": "device_moderate_speeding_auto_add_to_coaching",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether moderate speeding alerts are added to coaching or not"
        },
    },
    {
        "name": "device_heavy_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {"comment": "Speed threshold for heavy speeding alerts"},
    },
    {
        "name": "device_heavy_speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before alert for heavy speeding alerts"},
    },
    {
        "name": "device_heavy_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether heavy speeding alerts are enabled for the device or not"
        },
    },
    {
        "name": "device_heavy_speeding_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether heavy speeding alerts are sent to Safety Inbox or not"
        },
    },
    {
        "name": "device_heavy_speeding_auto_add_to_coaching",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether heavy speeding alerts are added to coaching or not"
        },
    },
    {
        "name": "device_severe_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {"comment": "Speed threshold for severe speeding alerts"},
    },
    {
        "name": "device_severe_speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before alert for severe speeding alerts"},
    },
    {
        "name": "device_severe_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether severe speeding alerts are enabled for the device or not"
        },
    },
    {
        "name": "device_severe_speeding_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether severe speeding alerts go to Safety Inbox or not"
        },
    },
    {
        "name": "device_severe_speeding_auto_add_to_coaching",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether severe speeding alerts are added to coaching or not"
        },
    },
    {
        "name": "device_max_speed_in_cab_audio_alerts_disabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether max speed in-cab audio alerts are disabled or not"
        },
    },
    {
        "name": "device_max_speed_threshold_milliknots",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Threshold for max speed in milliknots"
        },
    },
    {
        "name": "device_max_speed_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Time (in milliseconds) before alert for max speed alerts"
        },
    },
    {
        "name": "device_harsh_accel_setting",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Harsh acceleration setting for the device"
        },
    },
    {
        "name": "device_crash_alert_disabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether crash in-cab alerts are disabled for the device"
        },
    },
    {
        "name": "device_harsh_driving_alert_disabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether harsh driving in-cab alerts are disabled for the device"
        },
    },
]

QUERY = """
with
devices as (
select dim_devices.device_id AS vg_device_id,
       dim_devices.serial AS vg_serial,
       dim_devices.product_id AS vg_product_id,
       products_vg.name AS vg_product_name,
       associated_devices.camera_device_id AS cm_device_id,
       associated_devices.camera_serial AS cm_serial,
       associated_devices.camera_product_id AS cm_product_id,
       CASE WHEN associated_devices.camera_product_id IN (25, 30, 44, 167) THEN 'outward'
            WHEN associated_devices.camera_product_id IN (31, 43, 155) THEN 'inward'
            END AS cm_type,
       products_cm.name AS cm_product_name,
       dim_devices.org_id,
       dim_devices.device_name AS vg_device_name,
       devices_cm.device_settings_proto.safety.policy_violation_settings.enabled as device_policy_violation_enabled,
       devices_cm.device_settings_proto.safety.policy_violation_settings.audio_alert_settings.enabled AS device_policy_violation_audio_alerts_enabled,
       devices_cm.device_settings_proto.safety.policy_violation_settings.phone_usage_enabled as device_phone_usage_enabled,
       devices_cm.device_settings_proto.safety.in_cab_stop_sign_violation_alert_settings.enabled as device_rolling_stop_enabled,
       devices_cm.device_settings_proto.safety.in_cab_stop_sign_violation_alert_settings.enable_audio_alerts as device_rolling_stop_audio_alerts_enabled,
       devices_cm.device_settings_proto.safety.in_cab_stop_sign_violation_alert_settings.stop_threshold_kmph as device_rolling_stop_threshold_kmph,
       devices_cm.device_settings_proto.safety.drowsiness_detection_settings.drowsiness_detection_enabled as device_drowsiness_enabled,
       devices_cm.device_settings_proto.safety.drowsiness_detection_settings.drowsiness_detection_audio_alerts_enabled as device_drowsiness_audio_alerts_enabled,
       devices_cm.device_settings_proto.safety.drowsiness_detection_settings.drowsiness_detection_sensitivity_level as device_drowsiness_detection_sensitivity_level,
       devices_cm.device_settings_proto.safety.in_cab_speed_limit_alert_settings.enable_audio_alerts as device_in_cab_speed_limit_audio_alerts_enabled,
       devices_cm.device_settings_proto.safety.in_cab_speed_limit_alert_settings.kmph_over_limit_enabled as device_in_cab_speed_limit_kmph_over_limit_enabled,
       devices_cm.device_settings_proto.safety.in_cab_speed_limit_alert_settings.kmph_over_limit_threshold as device_in_cab_speed_limit_kmph_over_limit_threshold,
       devices_cm.device_settings_proto.safety.in_cab_speed_limit_alert_settings.percent_over_limit_enabled as device_in_cab_speed_limit_percent_over_limit_enabled,
       devices_cm.device_settings_proto.safety.in_cab_speed_limit_alert_settings.percent_over_limit_threshold as device_in_cab_speed_limit_percent_over_limit_threshold,
       devices_cm.device_settings_proto.safety.in_cab_speed_limit_alert_settings.time_before_alert_ms as device_in_cab_speed_limit_time_before_alert_ms,
       devices_cm.device_settings_proto.distracted_driving_detection_safety_inbox_device_enabled AS device_distracted_driving_enabled,
       devices_cm.device_settings_proto.distracted_driving_detection_audio_alerts_device_enabled AS device_distracted_driving_audio_alerts_enabled,
       devices_cm.device_settings_proto.forward_collision_warning_safety_inbox_device_enabled as device_forward_collision_warning_enabled,
       devices_cm.device_settings_proto.forward_collision_warning_audio_alerts_device_enabled as device_forward_collision_warning_audio_alerts_enabled,
       devices_cm.device_settings_proto.safety.forward_collision_sensitivity_level as device_forward_collision_sensitivity_level,
       devices_cm.device_settings_proto.following_distance_safety_inbox_device_enabled as device_following_distance_enabled,
       devices_cm.device_settings_proto.following_distance_audio_alerts_device_enabled AS device_following_distance_audio_alerts_enabled,
       devices_cm.device_settings_proto.safety.policy_violation_settings.seatbelt_enabled AS device_seatbelt_enabled,
       devices_cm.device_settings_proto.safety.policy_violation_settings.audio_alert_settings.seatbelt_enabled AS device_seatbelt_audio_alerts_enabled,
       devices_cm.device_settings_proto.safety.policy_violation_settings.seatbelt_minimum_speed AS device_seatbelt_minimum_speed,
       devices_cm.device_settings_proto.safety.policy_violation_settings.camera_obstruction_enabled AS device_inward_obstruction_enabled,
       devices_cm.device_settings_proto.safety.policy_violation_settings.audio_alert_settings.camera_obstruction_enabled AS device_inward_obstruction_audio_alerts_enabled,
       devices_cm.device_settings_proto.safety.policy_violation_settings.inward_obstruction_minimum_speed AS device_inward_obstruction_minimum_speed,
       devices_vg.device_settings_proto.voice_coaching_alert_config.maximum_speed_alert_disabled as device_max_speed_in_cab_audio_alerts_disabled,
       devices_vg.device_settings_proto.voice_coaching_speed_threshold_milliknots as device_max_speed_threshold_milliknots,
       devices_vg.device_settings_proto.voice_coaching_alert_config.maximum_speed_time_before_alert_ms as device_max_speed_time_before_alert_ms,
       devices_vg.device_settings_proto.voice_coaching_alert_config.crash_alert_disabled as device_crash_alert_disabled,
       devices_vg.device_settings_proto.voice_coaching_alert_config.harsh_driving_alert_disabled as device_harsh_driving_alert_disabled,
       devices_vg.harsh_accel_setting as device_harsh_accel_setting
from datamodel_core.dim_devices dim_devices
left join datamodel_core_bronze.raw_productdb_devices devices_cm on devices_cm.id = dim_devices.associated_devices.camera_device_id and devices_cm.org_id = dim_devices.org_id and devices_cm.date = '{PARTITION_START}'
left join datamodel_core_bronze.raw_productdb_devices devices_vg on devices_vg.id = dim_devices.device_id and devices_vg.org_id = dim_devices.org_id and devices_vg.date = '{PARTITION_START}'
left join definitions.products products_cm on dim_devices.associated_devices.camera_product_id = products_cm.product_id
left join definitions.products products_vg on dim_devices.product_id = products_vg.product_id
where dim_devices.date = '{PARTITION_START}'
and dim_devices.device_type = 'VG - Vehicle Gateway'
),
safety_device_settings as (
SELECT device_id,
       org_id,
       updated_at as safety_setting_updated_at,
       created_at as safety_setting_created_at,
       device_setting.mobile_usage_settings.enabled as mobile_usage_enabled,
       device_setting.mobile_usage_settings.audio_alerts_enabled as mobile_usage_audio_alerts_enabled,
       device_setting.mobile_usage_settings.audio_alert_promotion_settings.enabled as mobile_usage_audio_alert_promotion_enabled,
       device_setting.distracted_driving_settings.audio_alert_promotion_settings.enabled as distracted_driving_audio_alert_promotion_enabled,
       device_setting.following_distance_settings.audio_alert_promotion_settings.enabled as following_distance_audio_alert_promotion_enabled,
       device_setting.eating_drinking_settings.audio_alert_promotion_settings.enabled as eating_drinking_audio_alert_promotion_enabled,
       device_setting.lane_departure_warning_settings.enabled as lane_departure_warning_enabled,
       device_setting.lane_departure_warning_settings.audio_alerts_enabled as lane_departure_warning_audio_alerts_enabled,
       device_setting.lane_departure_warning_settings.audio_alert_promotion_settings.enabled as lane_departure_warning_audio_alert_promotion_enabled,
       device_setting.ml_mode_settings.ml_mode as ml_mode,
       device_setting.seatbelt_settings.audio_alert_promotion_settings.enabled as seatbelt_audio_alert_promotion_enabled,
       device_setting.inward_obstruction_settings.audio_alert_promotion_settings.enabled as inward_obstruction_audio_alert_promotion_enabled,
       device_setting.recording_settings.external_camera as external_camera,
       device_setting.recording_settings.primary_camera as primary_camera,
       device_setting.recording_settings.driver_facing_camera as driver_facing_camera
FROM safetydb_shards.device_settings),
device_speeding_settings_pre AS (
    select *, row_number() over (partition by org_id, device_id order by created_at_ms desc) as rn
    from safetydb_shards.device_safety_settings
    where date <= '{PARTITION_START}'
),
device_speeding_settings AS (
    select
    org_id,
    device_id,
    settings.speeding_settings.csl_enabled AS device_csl_enabled,
    settings.speeding_settings.severity_settings_speed_over_limit_unit AS device_severity_settings_speed_over_limit_unit,
    settings.speeding_settings.max_speed_setting.enabled as device_max_speed_enabled,
    settings.speeding_settings.max_speed_setting.send_to_safety_inbox as device_max_speed_send_to_safety_inbox,
    settings.speeding_settings.max_speed_setting.auto_add_to_coaching as device_max_speed_auto_add_to_coaching,
    settings.speeding_settings.in_cab_severity_alert_setting.enabled as device_in_cab_severity_alert_enabled,
    settings.speeding_settings.in_cab_severity_alert_setting.alert_at_severity_level as device_in_cab_severity_alert_alert_at_severity_level,
    settings.speeding_settings.in_cab_severity_alert_setting.time_before_alert_ms as device_in_cab_severity_alert_time_before_alert_ms,
    settings.speeding_settings.severity_settings[0].speed_over_limit_threshold AS device_light_speeding_speed_over_limit_threshold,
    settings.speeding_settings.severity_settings[0].time_before_alert_ms as device_light_speeding_time_before_alert_ms,
    settings.speeding_settings.severity_settings[0].enabled as device_light_speeding_enabled,
    settings.speeding_settings.severity_settings[0].send_to_safety_inbox as device_light_speeding_send_to_safety_inbox,
    settings.speeding_settings.severity_settings[0].auto_add_to_coaching as device_light_speeding_auto_add_to_coaching,
    settings.speeding_settings.severity_settings[1].speed_over_limit_threshold AS device_moderate_speeding_speed_over_limit_threshold,
    settings.speeding_settings.severity_settings[1].time_before_alert_ms as device_moderate_speeding_time_before_alert_ms,
    settings.speeding_settings.severity_settings[1].enabled as device_moderate_speeding_enabled,
    settings.speeding_settings.severity_settings[1].send_to_safety_inbox as device_moderate_speeding_send_to_safety_inbox,
    settings.speeding_settings.severity_settings[1].auto_add_to_coaching as device_moderate_speeding_auto_add_to_coaching,
    settings.speeding_settings.severity_settings[2].speed_over_limit_threshold AS device_heavy_speeding_speed_over_limit_threshold,
    settings.speeding_settings.severity_settings[2].time_before_alert_ms as device_heavy_speeding_time_before_alert_ms,
    settings.speeding_settings.severity_settings[2].enabled as device_heavy_speeding_enabled,
    settings.speeding_settings.severity_settings[2].send_to_safety_inbox as device_heavy_speeding_send_to_safety_inbox,
    settings.speeding_settings.severity_settings[2].auto_add_to_coaching as device_heavy_speeding_auto_add_to_coaching,
    settings.speeding_settings.severity_settings[3].speed_over_limit_threshold AS device_severe_speeding_speed_over_limit_threshold,
    settings.speeding_settings.severity_settings[3].time_before_alert_ms as device_severe_speeding_time_before_alert_ms,
    settings.speeding_settings.severity_settings[3].enabled as device_severe_speeding_enabled,
    settings.speeding_settings.severity_settings[3].send_to_safety_inbox as device_severe_speeding_send_to_safety_inbox,
    settings.speeding_settings.severity_settings[3].auto_add_to_coaching as device_severe_speeding_auto_add_to_coaching
    from device_speeding_settings_pre
    where rn = 1
)
select '{PARTITION_START}' as date,
       devices.*,
       safety_device_settings.* EXCEPT (org_id, device_id),
       device_speeding_settings.* EXCEPT (org_id, device_id)
from devices
left join safety_device_settings
  on safety_device_settings.org_id = devices.org_id
  and safety_device_settings.device_id = devices.cm_device_id
left join device_speeding_settings
  on device_speeding_settings.org_id = devices.org_id
  and device_speeding_settings.device_id = devices.vg_device_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Safety settings unnested from safetydb_shards.device_settings""",
        row_meaning="""One organization safety device settings as of date.""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=["date", "org_id", "vg_device_id"],
    upstreams=[
        "safetydb_shards.device_settings",
        "datamodel_core_bronze.raw_productdb_devices",
        "datamodel_core.dim_devices",
        "safetydb_shards.device_safety_settings",
    ],
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_safety_device_settings"),
    ],
)
def stg_safety_device_settings(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_safety_device_settings")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
    )
    context.log.info(query)
    return query
