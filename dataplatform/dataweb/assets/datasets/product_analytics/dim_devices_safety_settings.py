from dagster import AssetExecutionContext, BackfillPolicy, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
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
    get_run_env,
    partition_key_ranges_from_context,
    get_all_regions
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
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.ORG_ID,
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
        "name": "vg_device_name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Device name of VG device"},
    },
    {
        "name": "vg_product_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Product ID of VG device"},
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
        "name": "cm_type",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Type of CM device (inward, outward)"},
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
        "metadata": {"comment": "Time at which safety settings were created"},
    },
    {
        "name": "mobile_usage_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether mobile usage is enabled for the device or not"
        },
    },
    {
        "name": "mobile_usage_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for mobile usage violations or not"
        },
    },
    {
        "name": "mobile_usage_audio_alert_promotion_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alert promotion enabled for mobile usage violations or not (from safety org settings table)"
        },
    },
    {
        "name": "mobile_usage_audio_alert_count_threshold",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Threshold for mobile usage audio alerts"},
    },
    {
        "name": "mobile_usage_audio_alert_duration_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Duration (in milliseconds) of mobile usage audio alerts"
        },
    },
    {
        "name": "mobile_usage_per_trip_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "Whether mobile usage is enabled per trip or not"},
    },
    {
        "name": "mobile_usage_minimum_speed_mph",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed (in MPH) at which mobile usage alerts get triggered"
        },
    },
    {
        "name": "policy_violations_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether policy violations are enabled for the device or not"
        },
    },
    {
        "name": "policy_violations_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts for policy violations are enabled for the device or not"
        },
    },
    {
        "name": "drowsiness_detection_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether drowsiness detection audio alerts are enabled for the device or not"
        },
    },
    {
        "name": "seatbelt_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether seatbelt alerts are enabled for the device or not"
        },
    },
    {
        "name": "seatbelt_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts for seatbelt violations are enabled for the device or not"
        },
    },
    {
        "name": "seatbelt_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed for setting off seatbelt alerts"
        },
    },
    {
        "name": "inward_obstruction_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether inward obstruction alerts are enabled for the device or not"
        },
    },
    {
        "name": "inward_obstruction_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts for inward obstruction violations are enabled for the device or not"
        },
    },
    {
        "name": "inward_obstruction_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed for setting off inward obstruction alerts"
        },
    },
    {
        "name": "drowsiness_detection_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether drowsiness detection is enabled for the device or not"
        },
    },
    {
        "name": "inattentive_driving_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether inattentive driving detection is enabled for the device or not"
        },
    },
    {
        "name": "inattentive_driving_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether inattentive driving audio alerts are enabled for the device or not"
        },
    },
    {
        "name": "inattentive_driving_audio_alert_promotion_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether inattentive driving promotion alerts are enabled for the device or not"
        },
    },
    {
        "name": "forward_collision_warning_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether forward collision warning detection is enabled for the device or not"
        },
    },
    {
        "name": "forward_collision_warning_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether forward collision warning audio alerts are enabled for the device or not"
        },
    },
    {
        "name": "following_distance_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether following distance detection is enabled for the device or not"
        },
    },
    {
        "name": "following_distance_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether following distance audio alerts are enabled for the device or not"
        },
    },
    {
        "name": "following_distance_audio_alert_promotion_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether following distance promotion alerts are enabled for the device or not"
        },
    },
    {
        "name": "lane_departure_warning_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether lane departure warning detection is enabled for the device or not"
        },
    },
    {
        "name": "lane_departure_warning_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether lane departure warning audio alerts are enabled for the device or not"
        },
    },
    {
        "name": "lane_departure_warning_audio_alert_promotion_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether lane departure warning promotion alerts are enabled for the device or not"
        },
    },
    {
        "name": "severe_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether severe speeding is enabled for the device or not"
        },
    },
    {
        "name": "speeding_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether speeding in-cab audio alerts are enabled for the device or not"
        },
    },
    {
        "name": "light_speeding_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab speeding audio alert threshold covers Light speeding events (in-cab fires at or before Light threshold)"
        },
    },
    {
        "name": "moderate_speeding_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab speeding audio alert threshold covers Moderate speeding events (in-cab fires at or before Moderate threshold)"
        },
    },
    {
        "name": "heavy_speeding_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab speeding audio alert threshold covers Heavy speeding events (in-cab fires at or before Heavy threshold)"
        },
    },
    {
        "name": "severe_speeding_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab speeding audio alert threshold covers Severe speeding events (in-cab fires at or before Severe threshold)"
        },
    },
    {
        "name": "severe_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {
            "comment": "Threshold at which severe speeding alerts are triggered"
        },
    },
    {
        "name": "severe_speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before alert for severe speeding violations"},
    },
    {
        "name": "severe_speeding_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether severe speeding alerts should go to Safety Inbox or not"
        },
    },
    {
        "name": "severe_speeding_auto_add_to_coaching",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether severe speeding alerts are added to coaching or not"
        },
    },
    {
        "name": "moderate_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {
            "comment": "Speed threshold at which to set off moderate speeding alerts"
        },
    },
    {
        "name": "moderate_speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before alert for moderate speeding violations"},
    },
    {
        "name": "moderate_speeding_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether moderate speeding alerts should go to Safety Inbox or not"
        },
    },
    {
        "name": "moderate_speeding_auto_add_to_coaching",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether moderate speeding alerts are added to coaching or not"
        },
    },
    {
        "name": "light_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {
            "comment": "Speed threshold at which to set off light speeding alerts"
        },
    },
    {
        "name": "light_speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before alert for light speeding violations"},
    },
    {
        "name": "light_speeding_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether light speeding alerts should go to Safety Inbox or not"
        },
    },
    {
        "name": "light_speeding_auto_add_to_coaching",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether light speeding alerts are added to coaching or not"
        },
    },
    {
        "name": "heavy_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {
            "comment": "Speed threshold at which to set off heavy speeding alerts"
        },
    },
    {
        "name": "heavy_speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before alert for heavy speeding violations"},
    },
    {
        "name": "heavy_speeding_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether heavy speeding alerts should go to Safety Inbox or not"
        },
    },
    {
        "name": "heavy_speeding_auto_add_to_coaching",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether heavy speeding alerts are added to coaching or not"
        },
    },
    {
        "name": "moderate_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has moderate speeding alerts enabled or not"
        },
    },
    {
        "name": "light_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has light speeding alerts enabled or not"
        },
    },
    {
        "name": "heavy_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has heavy speeding alerts enabled or not"
        },
    },
    {
        "name": "severity_settings_speed_over_limit_unit",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unit of measure for setting off severe speeding alerts (MPH, milliknots per hour, percentage)"
        },
    },
    {
        "name": "rolling_stop_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether rolling stop is enabled for the device or not"
        },
    },
    {
        "name": "rolling_stop_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether rolling stop audio alerts are enabled for the device or not"
        },
    },
    {
        "name": "camera_recording_mode",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Camera recording mode of the device"
        },
    },
    {
        "name": "max_speed_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before notifications fire for max speed alerts"},
    },
    {
        "name": "max_speed_threshold_milliknots",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Max speed threshold (in milliknots)"},
    },
    {
        "name": "max_speed_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether max speed alerts are sent to safety inbox or not"
        },
    },
    {
        "name": "max_speed_auto_add_to_coaching",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether max speed alerts are added to coaching or not"
        },
    },
    {
        "name": "max_speed_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether the maximum speed limit setting is enabled for the device"
        },
    },
    {
        "name": "csl_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether device is enabled for commercial speed limit (CSL) or not"
        },
    },
    {
        "name": "max_speed_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether device has in-cab audio alerts enabled for Max Speed or not"
        },
    },
    {
        "name": "harsh_accel_setting",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Harsh acceleration setting for the device"
        },
    },
    {
        "name": "harsh_driving_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether harsh driving is enabled for the device or not"
        },
    },
    {
        "name": "crash_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether crash in-cab alerts are enabled for the device"
        },
    },
    {
        "name": "harsh_driving_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether harsh driving in-cab alerts are enabled for the device"
        },
    },
]

QUERY = """
with orgs AS (
select *
from {product_analytics_source}.dim_organizations_safety_settings
where date = '{PARTITION_START}'),
devices AS (
select *
from {product_analytics_staging_source}.stg_safety_device_settings
where date = '{PARTITION_START}'),
base AS (
select '{PARTITION_START}' as date,
        devices.org_id,
        devices.vg_device_id,
        devices.vg_serial,
        devices.vg_device_name,
        devices.vg_product_id,
        devices.cm_device_id,
        devices.cm_serial,
        devices.cm_type,
        devices.safety_setting_updated_at,
        devices.safety_setting_created_at,
        devices.device_max_speed_in_cab_audio_alerts_disabled,
        orgs.max_speed_in_cab_audio_alerts_enabled,
        devices.device_crash_alert_disabled,
        orgs.crash_in_cab_audio_alerts_enabled,
        devices.device_harsh_driving_alert_disabled,
        orgs.harsh_driving_in_cab_audio_alerts_enabled,
        devices.device_harsh_accel_setting,
        devices.mobile_usage_audio_alerts_enabled AS device_mobile_usage_audio_alerts_enabled,
        orgs.mobile_usage_audio_alerts_enabled AS org_mobile_usage_audio_alerts_enabled,
        devices.mobile_usage_audio_alert_promotion_enabled AS device_mobile_usage_audio_alert_promotion_enabled,
        orgs.mobile_usage_audio_alert_promotion_enabled AS org_mobile_usage_audio_alert_promotion_enabled,
        orgs.inattentive_driving_audio_alert_promotion_enabled AS org_inattentive_driving_audio_alert_promotion_enabled,
        devices.distracted_driving_audio_alert_promotion_enabled AS device_inattentive_driving_audio_alert_promotion_enabled,
        orgs.following_distance_audio_alert_promotion_enabled AS org_following_distance_audio_alert_promotion_enabled,
        devices.following_distance_audio_alert_promotion_enabled AS device_following_distance_audio_alert_promotion_enabled,
        devices.lane_departure_warning_audio_alerts_enabled AS device_lane_departure_warning_audio_alerts_enabled,
        orgs.lane_departure_warning_audio_alerts_enabled AS org_lane_departure_warning_audio_alerts_enabled,
        orgs.lane_departure_warning_audio_alert_promotion_enabled AS org_lane_departure_warning_audio_alert_promotion_enabled,
        devices.lane_departure_warning_audio_alert_promotion_enabled AS device_lane_departure_warning_audio_alert_promotion_enabled,
        orgs.mobile_usage_audio_alert_count_threshold,
        orgs.mobile_usage_audio_alert_duration_ms,
        orgs.mobile_usage_per_trip_enabled,
        orgs.mobile_usage_minimum_speed_mph,
        orgs.seatbelt_minimum_speed_enum,
        orgs.inward_obstruction_minimum_speed_enum,
        devices.device_policy_violation_audio_alerts_enabled,
        orgs.policy_violations_audio_alerts_enabled,
        devices.device_seatbelt_audio_alerts_enabled,
        orgs.seatbelt_audio_alerts_enabled,
        devices.device_inward_obstruction_audio_alerts_enabled,
        orgs.inward_obstruction_audio_alerts_enabled,
        devices.device_drowsiness_audio_alerts_enabled,
        orgs.drowsiness_detection_audio_alerts_enabled,
        devices.device_distracted_driving_audio_alerts_enabled,
        orgs.inattentive_driving_audio_alerts_enabled,
        devices.device_forward_collision_warning_audio_alerts_enabled,
        orgs.forward_collision_warning_audio_alerts_enabled,
        devices.device_following_distance_audio_alerts_enabled,
        orgs.following_distance_audio_alerts_enabled,
        orgs.rolling_stop_enabled,
        orgs.in_cab_stop_sign_violation_audio_alerts_enabled,
        devices.device_rolling_stop_enabled,
        devices.device_rolling_stop_audio_alerts_enabled,
        CASE --device is enabled
            WHEN cm_type IS NOT NULL
                AND (devices.device_rolling_stop_enabled = TRUE) THEN TRUE
            --org is enabled and device inherits from org or has no setting
            WHEN cm_type IS NOT NULL
                AND orgs.rolling_stop_enabled  = TRUE
                    AND (devices.device_rolling_stop_enabled IS NULL
                    OR devices.device_rolling_stop_enabled = TRUE) THEN TRUE ELSE FALSE END AS device_is_rolling_stop_enabled,
        CASE --device is enabled
            WHEN cm_type = 'inward'
             AND (devices.device_phone_usage_enabled = TRUE OR
            devices.mobile_usage_enabled = 2) THEN TRUE
            --org is enabled and device inherits from org or has no setting
            WHEN cm_type = 'inward'
             AND orgs.mobile_usage_enabled = TRUE
                AND (devices.mobile_usage_enabled = 0
                  OR devices.mobile_usage_enabled IS NULL) THEN TRUE ELSE FALSE END AS mobile_usage_enabled,
        CASE --device is enabled
            WHEN cm_type = 'inward'
             AND (devices.device_policy_violation_enabled = TRUE) THEN TRUE
            --org is enabled and device inherits from org or has no setting
            WHEN cm_type = 'inward'
             AND orgs.policy_violations_enabled = TRUE
                AND (devices.device_policy_violation_enabled IS NULL
                  OR devices.device_policy_violation_enabled = TRUE) THEN TRUE ELSE FALSE END AS policy_violations_enabled,
        CASE --device is enabled
            WHEN cm_type = 'inward'
             AND (devices.device_seatbelt_enabled = TRUE) THEN TRUE
            --org is enabled and device inherits from org or has no setting
            WHEN cm_type = 'inward'
             AND orgs.seatbelt_enabled = TRUE
                AND (devices.device_seatbelt_enabled IS NULL
                  OR devices.device_seatbelt_enabled = TRUE) THEN TRUE ELSE FALSE END AS seatbelt_enabled,
        CASE --device is enabled
            WHEN cm_type = 'inward'
             AND (devices.device_inward_obstruction_enabled = TRUE) THEN TRUE
            --org is enabled and device inherits from org or has no setting
            WHEN cm_type = 'inward'
             AND orgs.inward_obstruction_enabled = TRUE
                AND (devices.device_inward_obstruction_enabled IS NULL
                  OR devices.device_inward_obstruction_enabled = TRUE) THEN TRUE ELSE FALSE END AS inward_obstruction_enabled,
        CASE --device is enabled
            WHEN cm_type = 'inward'
             AND (devices.device_drowsiness_enabled = TRUE) THEN TRUE
            --org is enabled and device inherits from org or has no setting
            WHEN cm_type = 'inward'
             AND orgs.drowsiness_detection_enabled  = TRUE
                AND (devices.device_drowsiness_enabled IS NULL
                  OR devices.device_drowsiness_enabled = TRUE) THEN TRUE ELSE FALSE END AS drowsiness_detection_enabled,
        CASE --device is enabled
            WHEN cm_type = 'inward'
             AND (devices.device_distracted_driving_enabled = TRUE) THEN TRUE
            --org is enabled and device inherits from org or has no setting
            WHEN cm_type = 'inward'
             AND orgs.inattentive_driving_enabled  = TRUE
                AND (devices.device_distracted_driving_enabled IS NULL
                  OR devices.device_distracted_driving_enabled = TRUE) THEN TRUE ELSE FALSE END AS inattentive_driving_enabled,
        CASE --device is enabled
            WHEN cm_type IS NOT NULL
                AND (devices.device_forward_collision_warning_enabled = TRUE) THEN TRUE
            --org is enabled and device inherits from org or has no setting
            WHEN cm_type IS NOT NULL
                AND orgs.forward_collision_warning_enabled = TRUE
                    AND (devices.device_forward_collision_warning_enabled IS NULL
                    OR devices.device_forward_collision_warning_enabled = TRUE) THEN TRUE ELSE FALSE END AS forward_collision_warning_enabled,
        CASE --device is enabled
            WHEN cm_type IS NOT NULL
                AND (devices.device_following_distance_enabled = TRUE) THEN TRUE
            --org is enabled and device inherits from org or has no setting
            WHEN cm_type IS NOT NULL
                AND orgs.following_distance_enabled  = TRUE
                    AND (devices.device_following_distance_enabled IS NULL
                    OR devices.device_following_distance_enabled = TRUE) THEN TRUE ELSE FALSE END AS following_distance_enabled,
        CASE --device is enabled
            WHEN cm_type IS NOT NULL
                AND (devices.lane_departure_warning_enabled = TRUE OR devices.lane_departure_warning_enabled = 2) THEN TRUE
            --org is enabled and device inherits from org or has no setting
            WHEN cm_type IS NOT NULL
                AND orgs.lane_departure_warning_enabled  = TRUE
                    AND (devices.lane_departure_warning_enabled IS NULL
                    OR devices.lane_departure_warning_enabled = 0) THEN TRUE ELSE FALSE END AS lane_departure_warning_enabled,
        orgs.severe_speeding_enabled,
        devices.device_severe_speeding_enabled,
        CASE WHEN devices.device_severe_speeding_enabled = TRUE THEN TRUE
        WHEN devices.device_severe_speeding_enabled IS NULL THEN orgs.severe_speeding_enabled
        ELSE FALSE END AS severe_speeding_enabled_new,
        CASE WHEN devices.device_in_cab_speed_limit_audio_alerts_enabled IS NOT NULL
            THEN devices.device_in_cab_speed_limit_audio_alerts_enabled
            ELSE orgs.speeding_in_cab_audio_alerts_enabled
        END AS speeding_in_cab_audio_alerts_enabled,
        orgs.severe_speeding_speed_over_limit_threshold,
        devices.device_severe_speeding_speed_over_limit_threshold,
        orgs.severe_speeding_time_before_alert_ms,
        devices.device_severe_speeding_time_before_alert_ms,
        orgs.severity_settings_speed_over_limit_unit,
        devices.device_severity_settings_speed_over_limit_unit,
        CASE WHEN devices.device_severity_settings_speed_over_limit_unit = 0 THEN 'invalid_unit'
             WHEN devices.device_severity_settings_speed_over_limit_unit = 1 THEN 'percentage'
             WHEN devices.device_severity_settings_speed_over_limit_unit = 2 THEN 'miles_per_hour'
             WHEN devices.device_severity_settings_speed_over_limit_unit = 3 THEN 'kilometers_per_hour'
             WHEN devices.device_severity_settings_speed_over_limit_unit = 4 THEN 'milliknots_per_hour' END AS devices_severity_settings_speed_over_limit_unit_text,
        orgs.severe_speeding_send_to_safety_inbox,
        devices.device_severe_speeding_send_to_safety_inbox,
        orgs.severe_speeding_auto_add_to_coaching,
        devices.device_severe_speeding_auto_add_to_coaching,
        orgs.max_speed_enabled,
        devices.device_max_speed_enabled,
        CASE WHEN devices.device_max_speed_enabled IS TRUE THEN TRUE
        WHEN devices.device_max_speed_enabled IS NULL THEN orgs.max_speed_enabled
        ELSE FALSE END AS max_speed_enabled_new,
        orgs.max_speed_time_before_alert_ms,
        devices.device_max_speed_time_before_alert_ms,
        orgs.max_speed_threshold_milliknots,
        devices.device_max_speed_threshold_milliknots,
        orgs.max_speed_send_to_safety_inbox,
        devices.device_max_speed_send_to_safety_inbox,
        orgs.max_speed_auto_add_to_coaching,
        devices.device_max_speed_auto_add_to_coaching,
        orgs.light_speeding_speed_over_limit_threshold,
        devices.device_light_speeding_speed_over_limit_threshold,
        orgs.light_speeding_time_before_alert_ms,
        devices.device_light_speeding_time_before_alert_ms,
        orgs.light_speeding_enabled,
        devices.device_light_speeding_enabled,
        CASE WHEN devices.device_light_speeding_enabled = TRUE THEN TRUE
        WHEN devices.device_light_speeding_enabled IS NULL THEN orgs.light_speeding_enabled
        ELSE FALSE END AS light_speeding_enabled_new,
        orgs.light_speeding_send_to_safety_inbox,
        devices.device_light_speeding_send_to_safety_inbox,
        orgs.light_speeding_auto_add_to_coaching,
        devices.device_light_speeding_auto_add_to_coaching,
        orgs.moderate_speeding_speed_over_limit_threshold,
        devices.device_moderate_speeding_speed_over_limit_threshold,
        orgs.moderate_speeding_time_before_alert_ms,
        devices.device_moderate_speeding_time_before_alert_ms,
        orgs.moderate_speeding_enabled,
        devices.device_moderate_speeding_enabled,
        CASE WHEN devices.device_moderate_speeding_enabled = TRUE THEN TRUE
        WHEN devices.device_moderate_speeding_enabled IS NULL THEN orgs.moderate_speeding_enabled
        ELSE FALSE END AS moderate_speeding_enabled_new,
        orgs.moderate_speeding_send_to_safety_inbox,
        devices.device_moderate_speeding_send_to_safety_inbox,
        orgs.moderate_speeding_auto_add_to_coaching,
        devices.device_moderate_speeding_auto_add_to_coaching,
        orgs.heavy_speeding_speed_over_limit_threshold,
        devices.device_heavy_speeding_speed_over_limit_threshold,
        orgs.heavy_speeding_time_before_alert_ms,
        devices.device_heavy_speeding_time_before_alert_ms,
        orgs.heavy_speeding_enabled,
        devices.device_heavy_speeding_enabled,
        CASE WHEN devices.device_heavy_speeding_enabled = TRUE THEN TRUE
        WHEN devices.device_heavy_speeding_enabled IS NULL THEN orgs.heavy_speeding_enabled
        ELSE FALSE END AS heavy_speeding_enabled_new,
        orgs.heavy_speeding_send_to_safety_inbox,
        devices.device_heavy_speeding_send_to_safety_inbox,
        orgs.heavy_speeding_auto_add_to_coaching,
        devices.device_heavy_speeding_auto_add_to_coaching,
        orgs.csl_enabled,
        devices.device_csl_enabled,
        devices.device_in_cab_speed_limit_audio_alerts_enabled,
        devices.device_in_cab_speed_limit_kmph_over_limit_enabled,
        devices.device_in_cab_speed_limit_kmph_over_limit_threshold,
        devices.device_in_cab_speed_limit_percent_over_limit_enabled,
        devices.device_in_cab_speed_limit_percent_over_limit_threshold,
        orgs.speeding_kmph_over_limit_enabled,
        orgs.speeding_kmph_over_limit_threshold,
        orgs.speeding_percent_over_limit_enabled,
        orgs.speeding_percent_over_limit_threshold,
        orgs.light_speeding_in_cab_audio_alerts_enabled,
        orgs.moderate_speeding_in_cab_audio_alerts_enabled,
        orgs.heavy_speeding_in_cab_audio_alerts_enabled,
        orgs.severe_speeding_in_cab_audio_alerts_enabled,
        CASE WHEN
            devices.external_camera = 2
            AND devices.primary_camera = 2
            AND devices.driver_facing_camera = 2
        THEN 'Full' -- All are enabled
        WHEN
            devices.external_camera = 2
            AND devices.primary_camera = 2
            AND devices.driver_facing_camera = 1
        THEN 'Driver' -- Driver is disabled
        WHEN
            devices.external_camera = 1
            AND devices.primary_camera = 1
            AND devices.driver_facing_camera = 1
        THEN 'Complete' -- All are disabled
        -- Use org setting otherwise (either inherited from org already or illegal combo)
        ELSE orgs.camera_recording_mode END AS camera_recording_mode
FROM devices
LEFT JOIN orgs ON orgs.org_id = devices.org_id)

SELECT
date,
org_id,
vg_device_id,
vg_serial,
vg_device_name,
vg_product_id,
cm_device_id,
cm_serial,
cm_type,
safety_setting_updated_at,
safety_setting_created_at,
mobile_usage_enabled,
inattentive_driving_enabled,
CASE WHEN max_speed_enabled_new = TRUE AND device_max_speed_in_cab_audio_alerts_disabled IS NOT NULL
     THEN NOT device_max_speed_in_cab_audio_alerts_disabled
     ELSE max_speed_enabled_new AND max_speed_in_cab_audio_alerts_enabled
END AS max_speed_in_cab_audio_alerts_enabled,
CASE WHEN cm_type = 'inward'
    AND ((device_distracted_driving_audio_alerts_enabled = TRUE)
    OR (inattentive_driving_audio_alerts_enabled = TRUE
    AND device_distracted_driving_audio_alerts_enabled IS NULL))
    AND inattentive_driving_enabled = TRUE THEN TRUE ELSE FALSE END AS inattentive_driving_audio_alerts_enabled,
CASE WHEN inattentive_driving_enabled = TRUE
      AND device_inattentive_driving_audio_alert_promotion_enabled = 2 THEN TRUE
     WHEN inattentive_driving_enabled = TRUE
      AND (device_inattentive_driving_audio_alert_promotion_enabled = 0 OR device_inattentive_driving_audio_alert_promotion_enabled IS NULL)
      AND org_inattentive_driving_audio_alert_promotion_enabled = TRUE THEN TRUE ELSE FALSE END AS inattentive_driving_audio_alert_promotion_enabled,
forward_collision_warning_enabled,
CASE WHEN cm_type IS NOT NULL
    AND ((device_forward_collision_warning_audio_alerts_enabled = TRUE)
    OR (forward_collision_warning_audio_alerts_enabled = TRUE
    AND device_forward_collision_warning_audio_alerts_enabled IS NULL))
    AND forward_collision_warning_enabled = TRUE THEN TRUE ELSE FALSE END AS forward_collision_warning_audio_alerts_enabled,
following_distance_enabled,
CASE WHEN cm_type IS NOT NULL
    AND ((device_following_distance_audio_alerts_enabled = TRUE)
    OR (following_distance_audio_alerts_enabled = TRUE
    AND device_following_distance_audio_alerts_enabled IS NULL))
    AND following_distance_enabled = TRUE THEN TRUE ELSE FALSE END AS following_distance_audio_alerts_enabled,
CASE WHEN following_distance_enabled = TRUE
      AND device_following_distance_audio_alert_promotion_enabled = 2 THEN TRUE
     WHEN following_distance_enabled = TRUE
      AND (device_following_distance_audio_alert_promotion_enabled = 0 OR device_following_distance_audio_alert_promotion_enabled IS NULL)
      AND org_following_distance_audio_alert_promotion_enabled = TRUE THEN TRUE ELSE FALSE END AS following_distance_audio_alert_promotion_enabled,
lane_departure_warning_enabled,
CASE WHEN cm_type IS NOT NULL AND device_lane_departure_warning_audio_alerts_enabled = 2 AND lane_departure_warning_enabled = TRUE THEN TRUE
     WHEN cm_type IS NOT NULL AND (device_lane_departure_warning_audio_alerts_enabled = 0 OR device_lane_departure_warning_audio_alerts_enabled IS NULL)
      AND org_lane_departure_warning_audio_alerts_enabled = TRUE AND lane_departure_warning_enabled = TRUE THEN TRUE ELSE FALSE END AS lane_departure_warning_audio_alerts_enabled,
CASE WHEN lane_departure_warning_enabled = TRUE
      AND device_lane_departure_warning_audio_alert_promotion_enabled = 2 THEN TRUE
     WHEN lane_departure_warning_enabled = TRUE
      AND (device_lane_departure_warning_audio_alert_promotion_enabled = 0 OR device_lane_departure_warning_audio_alert_promotion_enabled IS NULL)
      AND org_lane_departure_warning_audio_alert_promotion_enabled = TRUE THEN TRUE ELSE FALSE END AS lane_departure_warning_audio_alert_promotion_enabled,
CASE WHEN cm_type = 'inward' AND device_mobile_usage_audio_alerts_enabled = 2 AND mobile_usage_enabled = TRUE THEN TRUE
     WHEN cm_type = 'inward' AND (device_mobile_usage_audio_alerts_enabled = 0 OR device_mobile_usage_audio_alerts_enabled IS NULL)
      AND org_mobile_usage_audio_alerts_enabled = TRUE AND mobile_usage_enabled = TRUE THEN TRUE ELSE FALSE END AS mobile_usage_audio_alerts_enabled,
CASE WHEN mobile_usage_enabled = TRUE
      AND device_mobile_usage_audio_alert_promotion_enabled = 2 THEN TRUE
     WHEN mobile_usage_enabled = TRUE
      AND (device_mobile_usage_audio_alert_promotion_enabled = 0 OR device_mobile_usage_audio_alert_promotion_enabled IS NULL)
      AND org_mobile_usage_audio_alert_promotion_enabled = TRUE THEN TRUE ELSE FALSE END AS mobile_usage_audio_alert_promotion_enabled,
CASE WHEN mobile_usage_enabled = TRUE
      AND device_mobile_usage_audio_alert_promotion_enabled = 2 THEN mobile_usage_audio_alert_count_threshold
     WHEN mobile_usage_enabled = TRUE
      AND (device_mobile_usage_audio_alert_promotion_enabled = 0 OR device_mobile_usage_audio_alert_promotion_enabled IS NULL)
      AND org_mobile_usage_audio_alert_promotion_enabled = TRUE THEN mobile_usage_audio_alert_count_threshold
      ELSE NULL END AS mobile_usage_audio_alert_count_threshold,
CASE WHEN mobile_usage_enabled = TRUE THEN mobile_usage_audio_alert_duration_ms ELSE NULL END AS mobile_usage_audio_alert_duration_ms,
CASE WHEN mobile_usage_enabled = TRUE THEN mobile_usage_per_trip_enabled ELSE NULL END AS mobile_usage_per_trip_enabled,
CASE WHEN mobile_usage_enabled = TRUE
     THEN mobile_usage_minimum_speed_mph END AS mobile_usage_minimum_speed_mph,
policy_violations_enabled,
CASE WHEN cm_type = 'inward'
    AND ((device_policy_violation_audio_alerts_enabled = TRUE)
    OR (policy_violations_audio_alerts_enabled = TRUE
    AND device_policy_violation_audio_alerts_enabled IS NULL))
    AND policy_violations_enabled = TRUE THEN TRUE ELSE FALSE END AS policy_violations_audio_alerts_enabled,
seatbelt_enabled,
CASE WHEN cm_type = 'inward'
    AND ((device_seatbelt_audio_alerts_enabled = TRUE)
    OR (seatbelt_audio_alerts_enabled = TRUE
    AND device_seatbelt_audio_alerts_enabled IS NULL))
    AND policy_violations_audio_alerts_enabled = TRUE
    AND seatbelt_enabled = TRUE THEN TRUE ELSE FALSE END AS seatbelt_audio_alerts_enabled,
CASE WHEN seatbelt_enabled = TRUE
    THEN seatbelt_minimum_speed_enum END AS seatbelt_minimum_speed_enum,
inward_obstruction_enabled,
CASE WHEN cm_type = 'inward'
    AND ((device_inward_obstruction_audio_alerts_enabled = TRUE)
    OR (inward_obstruction_audio_alerts_enabled = TRUE
    AND device_inward_obstruction_audio_alerts_enabled IS NULL))
    AND policy_violations_audio_alerts_enabled = TRUE
    AND inward_obstruction_enabled = TRUE THEN TRUE ELSE FALSE END AS inward_obstruction_audio_alerts_enabled,
CASE WHEN inward_obstruction_enabled = TRUE
    THEN inward_obstruction_minimum_speed_enum END AS inward_obstruction_minimum_speed_enum,
drowsiness_detection_enabled,
CASE WHEN cm_type = 'inward'
    AND ((device_drowsiness_audio_alerts_enabled = TRUE)
    OR (drowsiness_detection_audio_alerts_enabled = TRUE
    AND device_drowsiness_audio_alerts_enabled IS NULL))
    AND drowsiness_detection_enabled = TRUE THEN TRUE ELSE FALSE END AS drowsiness_detection_audio_alerts_enabled,
device_is_rolling_stop_enabled AS rolling_stop_enabled,
CASE WHEN cm_type IS NOT NULL
    AND ((device_rolling_stop_audio_alerts_enabled = TRUE)
    OR (in_cab_stop_sign_violation_audio_alerts_enabled = TRUE
    AND device_rolling_stop_audio_alerts_enabled IS NULL))
    AND device_is_rolling_stop_enabled = TRUE THEN TRUE ELSE FALSE END AS rolling_stop_audio_alerts_enabled,
severe_speeding_enabled_new AS severe_speeding_enabled,
CASE WHEN severe_speeding_enabled_new = TRUE AND device_severe_speeding_send_to_safety_inbox = TRUE
THEN TRUE
WHEN severe_speeding_enabled_new = TRUE AND device_severe_speeding_send_to_safety_inbox IS NULL THEN severe_speeding_send_to_safety_inbox
ELSE FALSE END AS severe_speeding_send_to_safety_inbox,
CASE WHEN severe_speeding_enabled_new = TRUE AND device_severe_speeding_auto_add_to_coaching = TRUE
THEN TRUE
WHEN severe_speeding_enabled_new = TRUE AND device_severe_speeding_auto_add_to_coaching IS NULL THEN severe_speeding_auto_add_to_coaching
ELSE FALSE END AS severe_speeding_auto_add_to_coaching,
speeding_in_cab_audio_alerts_enabled,
-- In-cab speeding audio alerts enabled for each severity level
-- Enabled if in_cab_threshold <= severity_threshold (in-cab fires at or before the severity event)
-- Uses device settings if device has kmph OR percent over limit enabled (mutually exclusive), otherwise uses org settings
-- Unit conversions: 1 mph = 1.60934 kmph, 1 milliknot = 0.001852 kmph
CASE
    -- Check if overall in-cab audio alerts are enabled (device or org)
    WHEN COALESCE(light_speeding_enabled_new, FALSE) = FALSE THEN FALSE
    WHEN COALESCE(speeding_in_cab_audio_alerts_enabled, FALSE) = FALSE THEN FALSE
    -- Device has percent over limit enabled (kmph and percent are mutually exclusive)
    WHEN device_in_cab_speed_limit_percent_over_limit_enabled = TRUE THEN
        CASE
            -- Both use percentage (device unit takes precedence over org unit)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'percentage'
                THEN device_in_cab_speed_limit_percent_over_limit_threshold <=
                    COALESCE(device_light_speeding_speed_over_limit_threshold, light_speeding_speed_over_limit_threshold)
            ELSE FALSE
        END
    -- Device has kmph over limit enabled
    WHEN device_in_cab_speed_limit_kmph_over_limit_enabled = TRUE THEN
        CASE
            -- Device in-cab uses kmph, severity uses kmph (device unit takes precedence over org unit)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'kilometers_per_hour'
                THEN device_in_cab_speed_limit_kmph_over_limit_threshold <=
                    COALESCE(device_light_speeding_speed_over_limit_threshold, light_speeding_speed_over_limit_threshold)
            -- Device in-cab uses kmph, severity uses mph (convert in-cab kmph to mph)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'miles_per_hour'
                THEN ROUND(device_in_cab_speed_limit_kmph_over_limit_threshold / 1.60934) <=
                    COALESCE(device_light_speeding_speed_over_limit_threshold, light_speeding_speed_over_limit_threshold)
            -- Device in-cab uses kmph, severity uses milliknots (convert severity milliknots to kmph)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'milliknots_per_hour'
                THEN ROUND(device_in_cab_speed_limit_kmph_over_limit_threshold) <=
                    ROUND(COALESCE(device_light_speeding_speed_over_limit_threshold, light_speeding_speed_over_limit_threshold) * 0.001852)
            ELSE FALSE
        END
    -- Device doesn't have its own threshold settings, use org settings
    ELSE COALESCE(light_speeding_in_cab_audio_alerts_enabled, FALSE)
END AS light_speeding_in_cab_audio_alerts_enabled,
CASE
    WHEN COALESCE(moderate_speeding_enabled_new, FALSE) = FALSE THEN FALSE
    -- Check if overall in-cab audio alerts are enabled (device or org)
    WHEN COALESCE(speeding_in_cab_audio_alerts_enabled, FALSE) = FALSE THEN FALSE
    -- Device has percent over limit enabled (kmph and percent are mutually exclusive)
    WHEN device_in_cab_speed_limit_percent_over_limit_enabled = TRUE THEN
        CASE
            -- Both use percentage (device unit takes precedence over org unit)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'percentage'
                THEN device_in_cab_speed_limit_percent_over_limit_threshold <=
                    COALESCE(device_moderate_speeding_speed_over_limit_threshold, moderate_speeding_speed_over_limit_threshold)
            ELSE FALSE
        END
    -- Device has kmph over limit enabled
    WHEN device_in_cab_speed_limit_kmph_over_limit_enabled = TRUE THEN
        CASE
            -- Device in-cab uses kmph, severity uses kmph (device unit takes precedence over org unit)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'kilometers_per_hour'
                THEN device_in_cab_speed_limit_kmph_over_limit_threshold <=
                    COALESCE(device_moderate_speeding_speed_over_limit_threshold, moderate_speeding_speed_over_limit_threshold)
            -- Device in-cab uses kmph, severity uses mph (convert in-cab kmph to mph)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'miles_per_hour'
                THEN ROUND(device_in_cab_speed_limit_kmph_over_limit_threshold / 1.60934) <=
                    COALESCE(device_moderate_speeding_speed_over_limit_threshold, moderate_speeding_speed_over_limit_threshold)
            -- Device in-cab uses kmph, severity uses milliknots (convert severity milliknots to kmph)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'milliknots_per_hour'
                THEN ROUND(device_in_cab_speed_limit_kmph_over_limit_threshold) <=
                    ROUND(COALESCE(device_moderate_speeding_speed_over_limit_threshold, moderate_speeding_speed_over_limit_threshold) * 0.001852)
            ELSE FALSE
        END
    -- Device doesn't have its own threshold settings, use org settings
    ELSE COALESCE(moderate_speeding_in_cab_audio_alerts_enabled, FALSE)
END AS moderate_speeding_in_cab_audio_alerts_enabled,
CASE
    WHEN COALESCE(heavy_speeding_enabled_new, FALSE) = FALSE THEN FALSE
    -- Check if overall in-cab audio alerts are enabled (device or org)
    WHEN COALESCE(speeding_in_cab_audio_alerts_enabled, FALSE) = FALSE THEN FALSE
    -- Device has percent over limit enabled (kmph and percent are mutually exclusive)
    WHEN device_in_cab_speed_limit_percent_over_limit_enabled = TRUE THEN
        CASE
            -- Both use percentage (device unit takes precedence over org unit)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'percentage'
                THEN device_in_cab_speed_limit_percent_over_limit_threshold <=
                    COALESCE(device_heavy_speeding_speed_over_limit_threshold, heavy_speeding_speed_over_limit_threshold)
            ELSE FALSE
        END
    -- Device has kmph over limit enabled
    WHEN device_in_cab_speed_limit_kmph_over_limit_enabled = TRUE THEN
        CASE
            -- Device in-cab uses kmph, severity uses kmph (device unit takes precedence over org unit)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'kilometers_per_hour'
                THEN device_in_cab_speed_limit_kmph_over_limit_threshold <=
                    COALESCE(device_heavy_speeding_speed_over_limit_threshold, heavy_speeding_speed_over_limit_threshold)
            -- Device in-cab uses kmph, severity uses mph (convert in-cab kmph to mph)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'miles_per_hour'
                THEN ROUND(device_in_cab_speed_limit_kmph_over_limit_threshold / 1.60934) <=
                    COALESCE(device_heavy_speeding_speed_over_limit_threshold, heavy_speeding_speed_over_limit_threshold)
            -- Device in-cab uses kmph, severity uses milliknots (convert severity milliknots to kmph)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'milliknots_per_hour'
                THEN ROUND(device_in_cab_speed_limit_kmph_over_limit_threshold) <=
                    ROUND(COALESCE(device_heavy_speeding_speed_over_limit_threshold, heavy_speeding_speed_over_limit_threshold) * 0.001852)
            ELSE FALSE
        END
    -- Device doesn't have its own threshold settings, use org settings
    ELSE COALESCE(heavy_speeding_in_cab_audio_alerts_enabled, FALSE)
END AS heavy_speeding_in_cab_audio_alerts_enabled,
CASE
    WHEN COALESCE(severe_speeding_enabled_new, FALSE) = FALSE THEN FALSE
    -- Check if overall in-cab audio alerts are enabled (device or org)
    WHEN COALESCE(speeding_in_cab_audio_alerts_enabled, FALSE) = FALSE THEN FALSE
    -- Device has percent over limit enabled (kmph and percent are mutually exclusive)
    WHEN device_in_cab_speed_limit_percent_over_limit_enabled = TRUE THEN
        CASE
            -- Both use percentage (device unit takes precedence over org unit)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'percentage'
                THEN device_in_cab_speed_limit_percent_over_limit_threshold <=
                    COALESCE(device_severe_speeding_speed_over_limit_threshold, severe_speeding_speed_over_limit_threshold)
            ELSE FALSE
        END
    -- Device has kmph over limit enabled
    WHEN device_in_cab_speed_limit_kmph_over_limit_enabled = TRUE THEN
        CASE
            -- Device in-cab uses kmph, severity uses kmph (device unit takes precedence over org unit)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'kilometers_per_hour'
                THEN device_in_cab_speed_limit_kmph_over_limit_threshold <=
                    COALESCE(device_severe_speeding_speed_over_limit_threshold, severe_speeding_speed_over_limit_threshold)
            -- Device in-cab uses kmph, severity uses mph (convert in-cab kmph to mph)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'miles_per_hour'
                THEN ROUND(device_in_cab_speed_limit_kmph_over_limit_threshold / 1.60934) <=
                    COALESCE(device_severe_speeding_speed_over_limit_threshold, severe_speeding_speed_over_limit_threshold)
            -- Device in-cab uses kmph, severity uses milliknots (convert severity milliknots to kmph)
            WHEN COALESCE(devices_severity_settings_speed_over_limit_unit_text, severity_settings_speed_over_limit_unit) = 'milliknots_per_hour'
                THEN ROUND(device_in_cab_speed_limit_kmph_over_limit_threshold) <=
                    ROUND(COALESCE(device_severe_speeding_speed_over_limit_threshold, severe_speeding_speed_over_limit_threshold) * 0.001852)
            ELSE FALSE
        END
    -- Device doesn't have its own threshold settings, use org settings
    ELSE COALESCE(severe_speeding_in_cab_audio_alerts_enabled, FALSE)
END AS severe_speeding_in_cab_audio_alerts_enabled,
CASE WHEN severe_speeding_enabled_new = TRUE AND device_severe_speeding_speed_over_limit_threshold IS NOT NULL
THEN device_severe_speeding_speed_over_limit_threshold
WHEN severe_speeding_enabled_new = TRUE THEN severe_speeding_speed_over_limit_threshold
ELSE NULL END AS severe_speeding_speed_over_limit_threshold,
CASE WHEN device_severity_settings_speed_over_limit_unit IS NOT NULL THEN devices_severity_settings_speed_over_limit_unit_text
ELSE severity_settings_speed_over_limit_unit END AS severity_settings_speed_over_limit_unit,
CASE WHEN severe_speeding_enabled_new = TRUE AND device_severe_speeding_time_before_alert_ms IS NOT NULL
THEN device_severe_speeding_time_before_alert_ms
WHEN severe_speeding_enabled_new = TRUE THEN severe_speeding_time_before_alert_ms
ELSE NULL END AS severe_speeding_time_before_alert_ms,
light_speeding_enabled_new AS light_speeding_enabled,
CASE WHEN light_speeding_enabled_new = TRUE AND device_light_speeding_send_to_safety_inbox = TRUE
THEN TRUE
WHEN light_speeding_enabled_new = TRUE AND device_light_speeding_send_to_safety_inbox IS NULL THEN light_speeding_send_to_safety_inbox
ELSE FALSE END AS light_speeding_send_to_safety_inbox,
CASE WHEN light_speeding_enabled_new = TRUE AND device_light_speeding_auto_add_to_coaching = TRUE
THEN TRUE
WHEN light_speeding_enabled_new = TRUE AND device_light_speeding_auto_add_to_coaching IS NULL THEN light_speeding_auto_add_to_coaching
ELSE FALSE END AS light_speeding_auto_add_to_coaching,
CASE WHEN light_speeding_enabled_new = TRUE AND device_light_speeding_speed_over_limit_threshold IS NOT NULL
THEN device_light_speeding_speed_over_limit_threshold
WHEN light_speeding_enabled_new = TRUE THEN light_speeding_speed_over_limit_threshold
ELSE NULL END AS light_speeding_speed_over_limit_threshold,
CASE WHEN light_speeding_enabled_new = TRUE AND device_light_speeding_time_before_alert_ms IS NOT NULL
THEN device_light_speeding_time_before_alert_ms
WHEN light_speeding_enabled_new = TRUE THEN light_speeding_time_before_alert_ms
ELSE NULL END AS light_speeding_time_before_alert_ms,
moderate_speeding_enabled_new AS moderate_speeding_enabled,
CASE WHEN moderate_speeding_enabled_new = TRUE AND device_moderate_speeding_send_to_safety_inbox = TRUE
THEN TRUE
WHEN moderate_speeding_enabled_new = TRUE AND device_moderate_speeding_send_to_safety_inbox IS NULL THEN moderate_speeding_send_to_safety_inbox
ELSE FALSE END AS moderate_speeding_send_to_safety_inbox,
CASE WHEN moderate_speeding_enabled_new = TRUE AND device_moderate_speeding_auto_add_to_coaching = TRUE
THEN TRUE
WHEN moderate_speeding_enabled_new = TRUE AND device_moderate_speeding_auto_add_to_coaching IS NULL THEN moderate_speeding_auto_add_to_coaching
ELSE FALSE END AS moderate_speeding_auto_add_to_coaching,
CASE WHEN moderate_speeding_enabled_new = TRUE AND device_moderate_speeding_speed_over_limit_threshold IS NOT NULL
THEN device_moderate_speeding_speed_over_limit_threshold
WHEN moderate_speeding_enabled_new = TRUE THEN moderate_speeding_speed_over_limit_threshold
ELSE NULL END AS moderate_speeding_speed_over_limit_threshold,
CASE WHEN moderate_speeding_enabled_new = TRUE AND device_moderate_speeding_time_before_alert_ms IS NOT NULL
THEN device_moderate_speeding_time_before_alert_ms
WHEN moderate_speeding_enabled_new = TRUE THEN moderate_speeding_time_before_alert_ms
ELSE NULL END AS moderate_speeding_time_before_alert_ms,
heavy_speeding_enabled_new AS heavy_speeding_enabled,
CASE WHEN heavy_speeding_enabled_new = TRUE AND device_heavy_speeding_send_to_safety_inbox = TRUE
THEN TRUE
WHEN heavy_speeding_enabled_new = TRUE AND device_heavy_speeding_send_to_safety_inbox IS NULL THEN heavy_speeding_send_to_safety_inbox
ELSE FALSE END AS heavy_speeding_send_to_safety_inbox,
CASE WHEN heavy_speeding_enabled_new = TRUE AND device_heavy_speeding_auto_add_to_coaching = TRUE
THEN TRUE
WHEN heavy_speeding_enabled_new = TRUE AND device_heavy_speeding_auto_add_to_coaching IS NULL THEN heavy_speeding_auto_add_to_coaching
ELSE FALSE END AS heavy_speeding_auto_add_to_coaching,
CASE WHEN heavy_speeding_enabled_new = TRUE AND device_heavy_speeding_speed_over_limit_threshold IS NOT NULL
THEN device_heavy_speeding_speed_over_limit_threshold
WHEN heavy_speeding_enabled_new = TRUE THEN heavy_speeding_speed_over_limit_threshold
ELSE NULL END AS heavy_speeding_speed_over_limit_threshold,
CASE WHEN heavy_speeding_enabled_new = TRUE AND device_heavy_speeding_time_before_alert_ms IS NOT NULL
THEN device_heavy_speeding_time_before_alert_ms
WHEN heavy_speeding_enabled_new = TRUE THEN heavy_speeding_time_before_alert_ms
ELSE NULL END AS heavy_speeding_time_before_alert_ms,
CASE WHEN device_csl_enabled IS TRUE THEN TRUE
WHEN device_csl_enabled IS NULL THEN csl_enabled
ELSE FALSE END AS csl_enabled,
max_speed_enabled_new AS max_speed_enabled,
CASE WHEN max_speed_enabled_new = TRUE AND device_max_speed_send_to_safety_inbox IS TRUE THEN TRUE
WHEN max_speed_enabled_new = TRUE AND device_max_speed_send_to_safety_inbox IS NULL THEN max_speed_send_to_safety_inbox
ELSE FALSE END AS max_speed_send_to_safety_inbox,
CASE WHEN max_speed_enabled_new = TRUE AND device_max_speed_auto_add_to_coaching IS TRUE THEN TRUE
WHEN max_speed_enabled_new = TRUE AND device_max_speed_auto_add_to_coaching IS NULL THEN max_speed_auto_add_to_coaching
ELSE FALSE END AS max_speed_auto_add_to_coaching,
CASE WHEN max_speed_enabled_new = TRUE AND device_max_speed_threshold_milliknots IS NOT NULL
THEN device_max_speed_threshold_milliknots
WHEN max_speed_enabled_new = TRUE THEN max_speed_threshold_milliknots
ELSE NULL END AS max_speed_threshold_milliknots,
CASE WHEN max_speed_enabled_new = TRUE AND device_max_speed_time_before_alert_ms IS NOT NULL
THEN device_max_speed_time_before_alert_ms
WHEN max_speed_enabled_new = TRUE THEN max_speed_time_before_alert_ms
ELSE NULL END AS max_speed_time_before_alert_ms,
CASE WHEN device_crash_alert_disabled IS NOT NULL
THEN NOT device_crash_alert_disabled
ELSE crash_in_cab_audio_alerts_enabled END AS crash_in_cab_audio_alerts_enabled,
CASE WHEN device_harsh_driving_alert_disabled IS NOT NULL
THEN NOT device_harsh_driving_alert_disabled
ELSE harsh_driving_in_cab_audio_alerts_enabled END AS harsh_driving_in_cab_audio_alerts_enabled,
CASE WHEN device_harsh_accel_setting != 3 THEN TRUE ELSE FALSE END AS harsh_driving_enabled,
device_harsh_accel_setting AS harsh_accel_setting,
camera_recording_mode
FROM base
"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""Device level safety settings with CM/VG devices paired in a single row. """,
        row_meaning="""Safety settings for one vg_device_id/cm_device_id pairing as of date.""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=["date", "org_id"],
    upstreams=[
        "product_analytics_staging.stg_safety_device_settings",
        "product_analytics.dim_organizations_safety_settings",
    ],
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_dim_devices_safety_settings"),
        NonNullDQCheck(
            name="dq_non_null_dim_devices_safety_settings",
            non_null_columns=["org_id", "vg_device_id", "mobile_usage_enabled"],
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_dim_devices_safety_settings",
            primary_keys=["date", "org_id", "vg_device_id"],
            block_before_write=True,
        ),
    ],
)
def dim_devices_safety_settings(context: AssetExecutionContext) -> str:
    context.log.info("Updating dim_devices_safety_settings")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    if get_run_env() == "dev":
        product_analytics_source = "datamodel_dev"
        product_analytics_staging_source = "datamodel_dev"
    else:
        product_analytics_source = "product_analytics"
        product_analytics_staging_source = "product_analytics_staging"
    query = QUERY.format(
        product_analytics_source=product_analytics_source,
        product_analytics_staging_source=product_analytics_staging_source,
        PARTITION_START=PARTITION_START,
    )
    context.log.info(query)
    return query
