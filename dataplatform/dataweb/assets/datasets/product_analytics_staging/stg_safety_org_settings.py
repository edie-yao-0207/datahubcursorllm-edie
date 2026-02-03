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
    get_run_env,
    get_all_regions,
    partition_key_ranges_from_context,
)
from pyspark.sql import SparkSession

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.DATE},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.ORG_ID},
    },
    {
        "name": "n_vg_devices",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Number of VG devices in the org"},
    },
    {
        "name": "n_cm_devices",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Number of CM devices in the org"},
    },
    {
        "name": "n_inward_cm_devices",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Number of inward-facing CM devices in the org"},
    },
    {
        "name": "n_outward_only_cm_devices",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Number of outward-only CM devices in the org"},
    },
    {
        "name": "eligible_severe_speeding",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one VG device"},
    },
    {
        "name": "eligible_following_distance",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one CM device"},
    },
    {
        "name": "eligible_outward_obstruction",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one CM device"},
    },
    {
        "name": "eligible_camera_misaligned",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one CM device"},
    },
    {
        "name": "eligible_forward_collision_warning",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one CM device"},
    },
    {
        "name": "eligible_rear_collision_warning",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one CM device"},
    },
    {
        "name": "eligible_rolling_stop",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one CM device"},
    },
    {
        "name": "eligible_distracted_driving",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one inward-facing CM device"},
    },
    {
        "name": "eligible_smoking",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one inward-facing CM device"},
    },
    {
        "name": "eligible_mask",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one inward-facing CM device"},
    },
    {
        "name": "eligible_eating_drinking",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one inward-facing CM device"},
    },
    {
        "name": "eligible_lane_departure",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one CM device"},
    },
    {
        "name": "eligible_mobile_usage",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one inward-facing CM device"},
    },
    {
        "name": "eligible_inward_obstruction",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one inward-facing CM device"},
    },
    {
        "name": "eligible_drowsiness",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one inward-facing CM device"},
    },
    {
        "name": "eligible_seatbelt",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if at least one inward-facing CM device"},
    },
    {
        "name": "speeding_setting_created_at_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Time at which speeding settings were created for the org"
        },
    },
    {
        "name": "csl_enabled",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether Commercial Speed Limits are enabled for the org or not"
        },
    },
    {
        "name": "severity_settings_speed_over_limit_unit",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Unit of measure for tracking severe speeding alerts (MPH, milliknots per hour, percentage)"
        },
    },
    {
        "name": "speeding_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether speeding in-cab audio alerts are enabled or not"
        },
    },
    {
        "name": "max_speed_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "Whether max speed is enabled for the org or not"},
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
        "name": "max_speed_in_cab_audio_alerts_disabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether max speed in-cab audio alerts are disabled or not"
        },
    },
    {
        "name": "in_cab_severity_alert_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab severity alerts are enabled for the org or not"
        },
    },
    {
        "name": "in_cab_severity_alert_alert_at_severity_level",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Level at which to raise in-cab alerts"},
    },
    {
        "name": "in_cab_severity_alert_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before in-cab alerts are raised"},
    },
    {
        "name": "light_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {"comment": "Threshold for light speeding alerts"},
    },
    {
        "name": "light_speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before alert for light speeding"},
    },
    {
        "name": "light_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether light speeding alerts are enabled for the org or not"
        },
    },
    {
        "name": "light_speeding_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether light speeding alerts are sent to Safety Inbox or not"
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
        "name": "moderate_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {"comment": "Speed threshold for moderate speeding alerts"},
    },
    {
        "name": "moderate_speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Time before which moderate speeding alerts are raised"
        },
    },
    {
        "name": "moderate_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether moderate speeding settings are enabled for the org or not"
        },
    },
    {
        "name": "moderate_speeding_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether moderate speeding alerts are sent to Safety Inbox or not"
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
        "name": "heavy_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {"comment": "Speed threshold for heavy speeding alerts"},
    },
    {
        "name": "heavy_speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before alert for heavy speeding alerts"},
    },
    {
        "name": "heavy_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether heavy speeding alerts are enabled for the org or not"
        },
    },
    {
        "name": "heavy_speeding_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether heavy speeding alerts are sent to Safety Inbox or not"
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
        "name": "severe_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {"comment": "Speed threshold for severe speeding alerts"},
    },
    {
        "name": "severe_speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time before alert for severe speeding alerts"},
    },
    {
        "name": "severe_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether severe speeding alerts are enabled for the org or not"
        },
    },
    {
        "name": "severe_speeding_send_to_safety_inbox",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether severe speeding alerts go to Safety Inbox or not"
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
        "name": "face_detection_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "Whether face detection is enabled for the org or not"},
    },
    {
        "name": "adas_all_features_allowed",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has all features allowed for alerts or not"
        },
    },
    {
        "name": "rolling_stop_enabled",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Whether org has rolling stop enabled or not"},
    },
    {
        "name": "rolling_stop_threshold_milliknots",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Threshold for rolling stop alerts (in milliknots)"},
    },
    {
        "name": "forward_collision_warning_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has forward collision warning alerts enabled or not"
        },
    },
    {
        "name": "forward_collision_warning_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alerts enabled for forward collision warning alerts or not"
        },
    },
    {
        "name": "forward_collision_sensitivity_level",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Sensitivity level for forward collision alerts"},
    },
    {
        "name": "distracted_driving_sensitivity_level",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Sensitivity level for distracted driving alerts"},
    },
    {
        "name": "distracted_driving_minimum_duration_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum duration at which to set off distracted driving alerts"
        },
    },
    {
        "name": "following_distance_minimum_duration_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum duration at which to see off following distance alerts"
        },
    },
    {
        "name": "following_distance_minimum_following_distance_seconds",
        "type": "float",
        "nullable": False,
        "metadata": {
            "comment": "Minimum following distance seconds at which to set off alerts"
        },
    },
    {
        "name": "policy_violations_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has policy violations alerts enabled or not"
        },
    },
    {
        "name": "policy_violation_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alerts enabled for policy violations or not"
        },
    },
    {
        "name": "policy_violation_minimum_speed",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed at which to set off policy violation alerts"
        },
    },
    {
        "name": "in_cab_stop_sign_violation_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "Whether org has stop sign violations enabled or not"},
    },
    {
        "name": "in_cab_stop_sign_violation_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alerts enabled for stop sign violations or not"
        },
    },
    {
        "name": "in_cab_stop_sign_violation_stop_threshold_kpmh",
        "type": "float",
        "nullable": False,
        "metadata": {"comment": "Speed threshold (in KMPH) for stop sign violations"},
    },
    {
        "name": "lane_depature_warning_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has lane departure warnings enabled or not"
        },
    },
    {
        "name": "lane_departure_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for lane departure violations or not"
        },
    },
    {
        "name": "drowsiness_detection_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has drowsiness detection alerts enabled or not"
        },
    },
    {
        "name": "drowsiness_detection_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alerts enabled for drowsiness detection violations or not"
        },
    },
    {
        "name": "drowsiness_detection_sensitivity_level",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum sensitivity level for drowsiness detection violations"
        },
    },
    {
        "name": "safety_setting_updated_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {"comment": "Last update time for safety settings"},
    },
    {
        "name": "mobile_usage_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has mobile usage alerts enabled or not (from safety org settings table)"
        },
    },
    {
        "name": "mobile_usage_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alerts enabled for mobile usage violations or not (from safety org settings table)"
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
        "name": "mobile_usage_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed at which to trigger mobile usage alerts"
        },
    },
    {
        "name": "mobile_usage_audio_alert_count_threshold",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Alert count threshold for mobile usage violations"},
    },
    {
        "name": "mobile_usage_audio_alert_duration_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Duration (in milliseconds) of audio alerts for mobile usage violations"
        },
    },
    {
        "name": "mobile_usage_per_trip_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether mobile usage violations are enabled per trip or not"
        },
    },
    {
        "name": "distracted_driving_detection_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether distracted driving violations are being tracked or not"
        },
    },
    {
        "name": "distracted_driving_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether distracted driving audio alerts are enabled or not"
        },
    },
    {
        "name": "distracted_driving_audio_alert_promotion_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alert promotion enabled for distracted driving alerts or not (from safety org settings table)"
        },
    },
    {
        "name": "distracted_driving_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed at which to trigger distracted driving alerts"
        },
    },
    {
        "name": "following_distance_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether following distance violations are being tracked or not"
        },
    },
    {
        "name": "following_distance_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for following distance violations or not"
        },
    },
    {
        "name": "following_distance_audio_alert_promotion_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alert promotion enabled or not for following distance violations (from safety org settings table)"
        },
    },
    {
        "name": "following_distance_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed at which to trigger following distance alerts"
        },
    },
    {
        "name": "eating_drinking_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether eating/drinking violations are being tracked or not"
        },
    },
    {
        "name": "eating_drinking_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for eating/drinking violations or not"
        },
    },
    {
        "name": "eating_drinking_audio_alert_promotion_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alert promotion enabled for eating/drinking violations or not (from safety org settings table)"
        },
    },
    {
        "name": "eating_drinking_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed at which to trigger eating/drinking alerts"
        },
    },
    {
        "name": "smoking_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether smoking violations are being tracked or not"
        },
    },
    {
        "name": "smoking_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for smoking violations or not"
        },
    },
    {
        "name": "mask_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether mask violations are being tracked or not"
        },
    },
    {
        "name": "mask_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for mask violations or not"
        },
    },
    {
        "name": "lane_departure_warning_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether lane departure warnings are enabled or not (from safety org settings table)"
        },
    },
    {
        "name": "lane_departure_warning_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alerts enabled for lane departure violations or not (from safety org settings table)"
        },
    },
    {
        "name": "lane_departure_warning_audio_alert_promotion_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alert promotion enabled for lane departure violations or not (from safety org settings table)"
        },
    },
    {
        "name": "lane_departure_warning_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed at which to trigger lane departure warning violations"
        },
    },
    {
        "name": "seatbelt_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "Whether seatbelt violations are being tracked or not"},
    },
    {
        "name": "seatbelt_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for seatbelt violations or not"
        },
    },
    {
        "name": "seatbelt_audio_alert_promotion_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alert promotion is enabled for seatbelt violations or not (from safety org settings table)"
        },
    },
    {
        "name": "seatbelt_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Minimum speed at which to trigger seatbelt alerts"},
    },
    {
        "name": "inward_obstruction_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether inward obstruction violations are being tracked or not"
        },
    },
    {
        "name": "inward_obstruction_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for inward obstruction violations or not"
        },
    },
    {
        "name": "inward_obstruction_audio_alert_promotion_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alert promotion is enabled for inward obstruction violations or not (from safety org settings)"
        },
    },
    {
        "name": "inward_obstruction_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed at which to trigger inward obstruction alerts"
        },
    },
    {
        "name": "speeding_kmph_over_limit_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether speeding alerts are based on KMPH or not"
        },
    },
    {
        "name": "speeding_kmph_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {
            "comment": "Threshold for KMPH speeding alerts"
        },
    },
    {
        "name": "speeding_percent_over_limit_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether speeding alerts are based on percent over the limit or not"
        },
    },
    {
        "name": "speeding_percent_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {
            "comment": "Threshold for percent over limit speeding alerts"
        },
    },
    {
        "name": "speeding_time_before_alert_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Time before alerts for speeding over the limit"
        },
    },
    {
        "name": "driverless_nudges_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether driverless nudges are enabled for the org or not"
        },
    },
    {
        "name": "camera_recording_mode",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Camera recording mode for the org"
        },
    },
    {
        "name": "crash_alert_disabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether crash in-cab alerts are disabled for the org or not"
        },
    },
    {
        "name": "harsh_driving_alert_disabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether harsh driving in-cab alerts are disabled for the org or not"
        },
    },
    {
        "name": "harsh_accel_passenger",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Passenger harsh accel sensitivity for the org"
        },
    },
    {
        "name": "harsh_accel_light_duty",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Light duty harsh accel sensitivity for the org"
        },
    },
    {
        "name": "harsh_accel_heavy_duty",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Heavy duty harsh accel sensitivity for the org"
        },
    },
    {
        "name": "harsh_brake_passenger",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Passenger harsh brake sensitivity for the org"
        },
    },
    {
        "name": "harsh_brake_light_duty",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Light duty harsh brake sensitivity for the org"
        },
    },
    {
        "name": "harsh_brake_heavy_duty",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Heavy duty harsh brake sensitivity for the org"
        },
    },
    {
        "name": "harsh_turn_passenger",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Passenger harsh turn sensitivity for the org"
        },
    },
    {
        "name": "harsh_turn_light_duty",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Light duty harsh turn sensitivity for the org"
        },
    },
    {
        "name": "harsh_turn_heavy_duty",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Heavy duty harsh turn sensitivity for the org"
        },
    },
]

QUERY = """
with org_devices AS (
select org_id,
       count(distinct vg_device_id) AS n_vg_devices,
       count(distinct case when cm_type is not null then cm_device_id end) as n_cm_devices,
       count(distinct case when cm_type = 'inward' then cm_device_id end) as n_inward_cm_devices,
       count(distinct case when cm_type = 'outward' then cm_device_id end) as n_outward_only_cm_devices
from {source}.stg_safety_device_settings
where date = '{PARTITION_START}'
group by 1),
org_eligible as (
select
       org_devices.*,
       case when org_devices.n_vg_devices > 0 then true else false end as eligible_severe_speeding,
       case when org_devices.n_cm_devices > 0 then true else false end as eligible_following_distance,
       case when org_devices.n_cm_devices > 0 then true else false end as eligible_outward_obstruction,
       case when org_devices.n_cm_devices > 0 then true else false end as eligible_camera_misaligned,
       case when org_devices.n_cm_devices > 0 then true else false end as eligible_forward_collision_warning,
       case when org_devices.n_cm_devices > 0 then true else false end as eligible_rear_collision_warning,
       case when org_devices.n_cm_devices > 0 then true else false end as eligible_rolling_stop,
       case when org_devices.n_cm_devices > 0 then true else false end as eligible_lane_departure,
       case when org_devices.n_inward_cm_devices > 0 then true else false end as eligible_distracted_driving,
       case when org_devices.n_inward_cm_devices > 0 then true else false end as eligible_smoking,
       case when org_devices.n_inward_cm_devices > 0 then true else false end as eligible_mask,
       case when org_devices.n_inward_cm_devices > 0 then true else false end as eligible_eating_drinking,
       case when org_devices.n_inward_cm_devices > 0 then true else false end as eligible_mobile_usage,
       case when org_devices.n_inward_cm_devices > 0 then true else false end as eligible_inward_obstruction,
       case when org_devices.n_inward_cm_devices > 0 then true else false end as eligible_drowsiness,
       case when org_devices.n_inward_cm_devices > 0 then true else false end as eligible_seatbelt
from org_devices),
org_settings as (
select id as org_id,
       settings_proto.face_detection_enabled,
       settings_proto.adas_all_features_allowed,
       CAST(rolling_stop_enabled AS integer) as rolling_stop_enabled,
       rolling_stop_threshold_milliknots as rolling_stop_threshold_milliknots,
       settings_proto.forward_collision_warning_enabled as forward_collision_warning_enabled,
       settings_proto.forward_collision_warning_audio_alerts_enabled as forward_collision_warning_audio_alerts_enabled,
       settings_proto.forward_collision_sensitivity_level as forward_collision_sensitivity_level,
       settings_proto.distracted_driving_sensitivity_level as distracted_driving_sensitivity_level,
       settings_proto.distracted_driving_detection_enabled AS distracted_driving_detection_enabled,
       settings_proto.distracted_driving_audio_alerts_enabled AS distracted_driving_audio_alerts_enabled,
       settings_proto.safety.distracted_driving_detection_settings.minimum_speed_enum as distracted_driving_minimum_speed_enum,
       settings_proto.safety.distracted_driving_detection_settings.minimum_duration_enum as distracted_driving_minimum_duration_enum,
       settings_proto.following_distance_enabled AS following_distance_enabled,
       settings_proto.following_distance_audio_alerts_enabled AS following_distance_audio_alerts_enabled,
       settings_proto.safety.following_distance_settings.minimum_duration_enum as following_distance_minimum_duration_enum,
       settings_proto.safety.following_distance_settings.minimum_following_distance_seconds as following_distance_minimum_following_distance_seconds,
       settings_proto.safety.following_distance_settings.minimum_speed_enum as following_distance_minimum_speed_enum,
       settings_proto.safety.policy_violation_settings.enabled as policy_violations_enabled,
       settings_proto.safety.policy_violation_settings.audio_alert_settings.enabled as policy_violation_audio_alerts_enabled,
       settings_proto.safety.policy_violation_settings.minimum_speed as policy_violation_minimum_speed,
       settings_proto.safety.policy_violation_settings.seatbelt_enabled as seatbelt_enabled,
       settings_proto.safety.policy_violation_settings.audio_alert_settings.seatbelt_enabled as seatbelt_audio_alerts_enabled,
       settings_proto.safety.policy_violation_settings.seatbelt_minimum_speed as seatbelt_minimum_speed_enum,
       settings_proto.safety.policy_violation_settings.food_enabled OR settings_proto.safety.policy_violation_settings.drink_enabled AS eating_drinking_enabled,
       settings_proto.safety.policy_violation_settings.audio_alert_settings.food_enabled OR settings_proto.safety.policy_violation_settings.audio_alert_settings.drink_enabled AS eating_drinking_audio_alerts_enabled,
       settings_proto.safety.policy_violation_settings.smoking_enabled AS smoking_enabled,
       settings_proto.safety.policy_violation_settings.audio_alert_settings.smoking_enabled AS smoking_audio_alerts_enabled,
       settings_proto.safety.policy_violation_settings.mask_enabled AS mask_enabled,
       settings_proto.safety.policy_violation_settings.audio_alert_settings.mask_enabled AS mask_audio_alerts_enabled,
       settings_proto.safety.in_cab_stop_sign_violation_alert_settings.enabled as in_cab_stop_sign_violation_enabled,
       settings_proto.safety.in_cab_stop_sign_violation_alert_settings.enable_audio_alerts as in_cab_stop_sign_violation_audio_alerts_enabled,
       settings_proto.safety.in_cab_stop_sign_violation_alert_settings.stop_threshold_kmph as in_cab_stop_sign_violation_stop_threshold_kpmh,
       settings_proto.safety.lane_departure_warning_settings.lane_departure_warning_enabled as lane_depature_warning_enabled,
       settings_proto.safety.lane_departure_warning_settings.lane_departure_warning_audio_alerts_enabled as lane_departure_audio_alerts_enabled,
       settings_proto.safety.drowsiness_detection_settings.drowsiness_detection_enabled as drowsiness_detection_enabled,
       settings_proto.safety.drowsiness_detection_settings.drowsiness_detection_audio_alerts_enabled as drowsiness_detection_audio_alerts_enabled,
       settings_proto.safety.drowsiness_detection_settings.drowsiness_detection_sensitivity_level as drowsiness_detection_sensitivity_level,
       settings_proto.safety.in_cab_speed_limit_alert_settings.enabled as speeding_in_cab_audio_alerts_enabled,
       settings_proto.safety.in_cab_speed_limit_alert_settings.kmph_over_limit_enabled as speeding_kmph_over_limit_enabled,
       settings_proto.safety.in_cab_speed_limit_alert_settings.kmph_over_limit_threshold as speeding_kmph_over_limit_threshold,
       settings_proto.safety.in_cab_speed_limit_alert_settings.percent_over_limit_enabled as speeding_percent_over_limit_enabled,
       settings_proto.safety.in_cab_speed_limit_alert_settings.percent_over_limit_threshold as speeding_percent_over_limit_threshold,
       settings_proto.safety.in_cab_speed_limit_alert_settings.time_before_alert_ms as speeding_time_before_alert_ms,
       settings_proto.safety.policy_violation_settings.camera_obstruction_enabled as inward_obstruction_enabled,
       settings_proto.safety.policy_violation_settings.audio_alert_settings.camera_obstruction_enabled as inward_obstruction_audio_alerts_enabled,
       settings_proto.safety.policy_violation_settings.inward_obstruction_minimum_speed as inward_obstruction_minimum_speed_enum,
       settings_proto.voice_coaching_alert_config.maximum_speed_alert_disabled as max_speed_in_cab_audio_alerts_disabled,
       settings_proto.voice_coaching_alert_config.crash_alert_disabled as crash_alert_disabled,
       settings_proto.voice_coaching_alert_config.harsh_driving_alert_disabled as harsh_driving_alert_disabled,
       voice_coaching_speed_threshold_milliknots as max_speed_threshold_milliknots,
       settings_proto.voice_coaching_alert_config.maximum_speed_time_before_alert_ms as max_speed_time_before_alert_ms
from datamodel_core_bronze.raw_clouddb_organizations
where date = '{PARTITION_START}'),
safety_org_settings AS (
select org_id,
       updated_at AS safety_setting_updated_at,
       CASE WHEN
           organization_setting.recording_settings.driver_facing_camera_disabled = TRUE
           AND organization_setting.recording_settings.external_camera_disabled = FALSE
           AND organization_setting.recording_settings.primary_camera_disabled = FALSE
       THEN 'Driver' -- Driver is disabled
       WHEN
           organization_setting.recording_settings.driver_facing_camera_disabled = TRUE
           AND organization_setting.recording_settings.external_camera_disabled = TRUE
           AND organization_setting.recording_settings.primary_camera_disabled = TRUE
       THEN 'Complete' -- All are disabled
       -- Full is default
       ELSE 'Full'
       END AS camera_recording_mode,
       organization_setting.mobile_usage_settings.enabled as mobile_usage_enabled,
       organization_setting.mobile_usage_settings.audio_alerts_enabled as mobile_usage_audio_alerts_enabled,
       organization_setting.mobile_usage_settings.audio_alert_promotion_settings.enabled as mobile_usage_audio_alert_promotion_enabled,
       organization_setting.mobile_usage_settings.minimum_speed_enum as mobile_usage_minimum_speed_enum,
       organization_setting.mobile_usage_settings.audio_alert_promotion_settings.alert_count_threshold AS mobile_usage_audio_alert_count_threshold,
       organization_setting.mobile_usage_settings.audio_alert_promotion_settings.duration_ms AS mobile_usage_audio_alert_duration_ms,
       organization_setting.mobile_usage_settings.audio_alert_promotion_settings.per_trip_enabled AS mobile_usage_per_trip_enabled,
       organization_setting.distracted_driving_settings.audio_alert_promotion_settings.enabled as distracted_driving_audio_alert_promotion_enabled,
       organization_setting.following_distance_settings.audio_alert_promotion_settings.enabled as following_distance_audio_alert_promotion_enabled,
       organization_setting.eating_drinking_settings.audio_alert_promotion_settings.enabled as eating_drinking_audio_alert_promotion_enabled,
       organization_setting.eating_drinking_settings.minimum_speed_enum as eating_drinking_minimum_speed_enum,
       organization_setting.lane_departure_warning_settings.enabled as lane_departure_warning_enabled,
       organization_setting.lane_departure_warning_settings.audio_alerts_enabled as lane_departure_warning_audio_alerts_enabled,
       organization_setting.lane_departure_warning_settings.audio_alert_promotion_settings.enabled as lane_departure_warning_audio_alert_promotion_enabled,
       organization_setting.lane_departure_warning_settings.minimum_speed_enum as lane_departure_warning_minimum_speed_enum,
       organization_setting.seatbelt_settings.audio_alert_promotion_settings.enabled as seatbelt_audio_alert_promotion_enabled,
       organization_setting.inward_obstruction_settings.audio_alert_promotion_settings.enabled as inward_obstruction_audio_alert_promotion_enabled,
       organization_setting.driverless_nudges_settings.driverless_nudges_enabled as driverless_nudges_enabled
from safetydb_shards.organization_settings),
harsh_settings AS (
select org_id,
       setting.advanced_settings.harsh_accel.passenger as harsh_accel_passenger,
       setting.advanced_settings.harsh_brake.passenger as harsh_brake_passenger,
       setting.advanced_settings.harsh_turn.passenger as harsh_turn_passenger,
       setting.advanced_settings.harsh_accel.light_duty as harsh_accel_light_duty,
       setting.advanced_settings.harsh_brake.light_duty as harsh_brake_light_duty,
       setting.advanced_settings.harsh_turn.light_duty as harsh_turn_light_duty,
       setting.advanced_settings.harsh_accel.heavy_duty as harsh_accel_heavy_duty,
       setting.advanced_settings.harsh_brake.heavy_duty as harsh_brake_heavy_duty,
       setting.advanced_settings.harsh_turn.heavy_duty as harsh_turn_heavy_duty
from safetydb_shards.harsh_events_v2_org_settings),
speeding_pre as (
select *,
      row_number() over (partition by org_id order by created_at_ms desc) as rnk
from safetydb_shards.org_safety_speeding_settings
where date <= '{PARTITION_START}'),
speeding_settings AS (
select
  org_id,
  created_at_ms as speeding_setting_created_at_ms,
  speeding_settings_proto.csl_enabled AS csl_enabled,
  speeding_settings_proto.severity_settings_speed_over_limit_unit AS severity_settings_speed_over_limit_unit,
  speeding_settings_proto.max_speed_setting.enabled as max_speed_enabled,
  speeding_settings_proto.max_speed_setting.send_to_safety_inbox as max_speed_send_to_safety_inbox,
  speeding_settings_proto.max_speed_setting.auto_add_to_coaching as max_speed_auto_add_to_coaching,
  speeding_settings_proto.in_cab_severity_alert_setting.enabled as in_cab_severity_alert_enabled,
  speeding_settings_proto.in_cab_severity_alert_setting.alert_at_severity_level as in_cab_severity_alert_alert_at_severity_level,
  speeding_settings_proto.in_cab_severity_alert_setting.time_before_alert_ms as in_cab_severity_alert_time_before_alert_ms,
  speeding_settings_proto.severity_settings[0].speed_over_limit_threshold AS light_speeding_speed_over_limit_threshold,
  speeding_settings_proto.severity_settings[0].time_before_alert_ms as light_speeding_time_before_alert_ms,
  speeding_settings_proto.severity_settings[0].enabled as light_speeding_enabled,
  speeding_settings_proto.severity_settings[0].send_to_safety_inbox as light_speeding_send_to_safety_inbox,
  speeding_settings_proto.severity_settings[0].auto_add_to_coaching as light_speeding_auto_add_to_coaching,
  speeding_settings_proto.severity_settings[1].speed_over_limit_threshold AS moderate_speeding_speed_over_limit_threshold,
  speeding_settings_proto.severity_settings[1].time_before_alert_ms as moderate_speeding_time_before_alert_ms,
  speeding_settings_proto.severity_settings[1].enabled as moderate_speeding_enabled,
  speeding_settings_proto.severity_settings[1].send_to_safety_inbox as moderate_speeding_send_to_safety_inbox,
  speeding_settings_proto.severity_settings[1].auto_add_to_coaching as moderate_speeding_auto_add_to_coaching,
  speeding_settings_proto.severity_settings[2].speed_over_limit_threshold AS heavy_speeding_speed_over_limit_threshold,
  speeding_settings_proto.severity_settings[2].time_before_alert_ms as heavy_speeding_time_before_alert_ms,
  speeding_settings_proto.severity_settings[2].enabled as heavy_speeding_enabled,
  speeding_settings_proto.severity_settings[2].send_to_safety_inbox as heavy_speeding_send_to_safety_inbox,
  speeding_settings_proto.severity_settings[2].auto_add_to_coaching as heavy_speeding_auto_add_to_coaching,
  speeding_settings_proto.severity_settings[3].speed_over_limit_threshold AS severe_speeding_speed_over_limit_threshold,
  speeding_settings_proto.severity_settings[3].time_before_alert_ms as severe_speeding_time_before_alert_ms,
  speeding_settings_proto.severity_settings[3].enabled as severe_speeding_enabled,
  speeding_settings_proto.severity_settings[3].send_to_safety_inbox as severe_speeding_send_to_safety_inbox,
  speeding_settings_proto.severity_settings[3].auto_add_to_coaching as severe_speeding_auto_add_to_coaching
from speeding_pre
where rnk = 1)

select '{PARTITION_START}' as date,
       org_eligible.*,
       speeding_settings.* EXCEPT (org_id),
       org_settings.* EXCEPT (org_id),
       safety_org_settings.* EXCEPT (org_id),
       harsh_settings.* EXCEPT (org_id)
from org_eligible
left join org_settings on org_settings.org_id = org_eligible.org_id
left join speeding_settings on speeding_settings.org_id = org_eligible.org_id
left join safety_org_settings on safety_org_settings.org_id = org_eligible.org_id
left join harsh_settings on harsh_settings.org_id = org_eligible.org_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Snapshot of safety org level settings without eligibility criteria. Includes speeding settings, policy violation settings, and configurations at the organization level.""",
        row_meaning="""One organization's safety settings as of date.""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=["date", "org_id"],
    upstreams=[
        "safetydb_shards.org_safety_speeding_settings",
        "product_analytics_staging.stg_safety_device_settings",
        "datamodel_core_bronze.raw_clouddb_organizations",
        "safetydb_shards.organization_settings",
        "safetydb_shards.harsh_events_v2_org_settings",
    ],
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_org_safety_settings"),
    ],
)
def stg_safety_org_settings(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_safety_org_settings")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    if get_run_env() == "dev":
        source = "datamodel_dev"
    else:
        source = "product_analytics_staging"
    query = QUERY.format(
        source=source,
        PARTITION_START=PARTITION_START,
    )
    context.log.info(query)
    return query
