from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
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
        "name": "adas_all_features_allowed",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "Whether org has all features allowed or not"},
    },
    {
        "name": "severe_speeding_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has severe speeding alerts enabled or not"
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
        "name": "speeding_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has in-cab audio alerts enabled or not for speeding violations"
        },
    },
    {
        "name": "severity_settings_speed_over_limit_unit_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Unit of measure for tracking severe speeding violations (enum)"
        },
    },
    {
        "name": "severity_settings_speed_over_limit_unit",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unit of measure for tracking severe speeding violations"
        },
    },
    {
        "name": "severe_speeding_speed_over_limit_threshold",
        "type": "float",
        "nullable": False,
        "metadata": {
            "comment": "Speed threshold at which to set off severe speeding alerts"
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
        "name": "distracted_driving_detection_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has any type of distracted driving alerts (inattentive driving, mobile usage, drowsiness detection) enabled or not"
        },
    },
    {
        "name": "inattentive_driving_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has inattentive driving alerts enabled or not"
        },
    },
    {
        "name": "inattentive_driving_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alerts enabled for inattentive driving violations or not"
        },
    },
    {
        "name": "inattentive_driving_audio_alert_promotion_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has promotion alerts enabled for inattentive driving violations or not"
        },
    },
    {
        "name": "inattentive_driving_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed (enum) for tracking inattentive driving violations"
        },
    },
    {
        "name": "policy_violations_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "Whether org has policy violations enabled or not"},
    },
    {
        "name": "policy_violations_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether org has audio alerts enabled or not for policy violations"
        },
    },
    {
        "name": "policy_violations_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Minimum speed (enum) for tracking policy violations"},
    },
    {
        "name": "policy_violations_minimum_speed_mph",
        "type": "integer",
        "nullable": False,
        "metadata": {"comment": "Minimum speed at which to set off policy violations"},
    },
    {
        "name": "mobile_usage_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "Whether org has mobile usage alerts enabled or not"},
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
        "metadata": {
            "comment": "Threshold for audio alerts for mobile usage violations"
        },
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
        "metadata": {"comment": "Whether mobile usage is enabled per trip or not"},
    },
    {
        "name": "mobile_usage_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed (enum) for tracking mobile usage violations"
        },
    },
    {
        "name": "mobile_usage_minimum_speed_mph",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed (in MPH) at which to set off mobile usage violations"
        },
    },
    {
        "name": "seatbelt_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether seatbelt violations are enabled or not for the org"
        },
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
            "comment": "Whether promotion alerts are enabled for seatbelt violations or not"
        },
    },
    {
        "name": "seatbelt_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed (enum) for triggering seatbelt violations"
        },
    },
    {
        "name": "rolling_stop_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether rolling stop violations are enabled or not for the org"
        },
    },
    {
        "name": "rolling_stop_threshold_milliknots",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Threshold (in milliknots) for rolling stop violations for the org"
        },
    },
    {
        "name": "in_cab_stop_sign_violation_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab stop sign violations are enabled for the org or not"
        },
    },
    {
        "name": "in_cab_stop_sign_violation_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for in-cab stop sign violations or not"
        },
    },
    {
        "name": "following_distance_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether following distance violations are enabled for the org or not"
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
            "comment": "Whether promotion alerts are enabled for following distance violations or not"
        },
    },
    {
        "name": "following_distance_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed (enum) for triggering following distance violations"
        },
    },
    {
        "name": "forward_collision_warning_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether forward collision warning alerts are enabled or not"
        },
    },
    {
        "name": "forward_collision_warning_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for forward collision violations or not"
        },
    },
    {
        "name": "lane_departure_warning_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether lane departure warning alerts are enabled or not"
        },
    },
    {
        "name": "lane_departure_warning_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for lane departure warning violations or not"
        },
    },
    {
        "name": "lane_departure_warning_audio_alert_promotion_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether promotion alerts are enabled for lane departure warning violations or not"
        },
    },
    {
        "name": "lane_departure_warning_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed (enum) for triggering lane departure warning violations"
        },
    },
    {
        "name": "inward_obstruction_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether inward obstruction alerts are enabled for the org or not"
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
            "comment": "Whether promotion alerts are enabled for inward obstruction violations or not"
        },
    },
    {
        "name": "inward_obstruction_minimum_speed_enum",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Minimum speed (enum) for triggering inward obstruction violations"
        },
    },
    {
        "name": "eating_drinking_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether eating/drinking alerts are enabled for the org or not"
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
        "name": "smoking_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether smoking alerts are enabled for the org or not"
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
            "comment": "Whether mask alerts are enabled for the org or not"
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
        "name": "drowsiness_detection_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether drowsiness detection alerts are enabled for the org or not"
        },
    },
    {
        "name": "drowsiness_detection_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether audio alerts are enabled for drowsiness detection violations or not"
        },
    },
    {
        "name": "max_speed_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether the maximum speed limit setting is enabled for the organization"
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
        "name": "csl_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether Commercial Speed Limits are enabled for the organization"
        },
    },
    {
        "name": "driverless_nudges_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether driverless nudges are enabled for the organization"
        },
    },
    {
        "name": "camera_recording_mode",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Camera recording mode of the org"
        },
    },
    {
        "name": "light_speeding_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab speeding audio alerts are enabled for Light speeding events (in-cab threshold >= Light threshold)"
        },
    },
    {
        "name": "moderate_speeding_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab speeding audio alerts are enabled for Moderate speeding events (in-cab threshold >= Moderate threshold)"
        },
    },
    {
        "name": "heavy_speeding_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab speeding audio alerts are enabled for Heavy speeding events (in-cab threshold >= Heavy threshold)"
        },
    },
    {
        "name": "severe_speeding_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab speeding audio alerts are enabled for Severe speeding events (in-cab threshold >= Severe threshold)"
        },
    },
    {
        "name": "max_speed_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether in-cab speeding audio alerts are enabled for Max Speed events"
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
        "name": "crash_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether crash in-cab audio alerts are enabled or not"
        },
    },
    {
        "name": "harsh_driving_in_cab_audio_alerts_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether harsh driving in-cab audio alerts are enabled or not"
        },
    },
    {
        "name": "harsh_accel_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether harsh acceleration alerts are enabled or not"
        },
    },
    {
        "name": "harsh_brake_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether harsh braking alerts are enabled or not"
        },
    },
    {
        "name": "harsh_turn_enabled",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether harsh turning alerts are enabled or not"
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
    WITH base as (
    select '{PARTITION_START}' as date,
        org_id,
        n_cm_devices,
        n_inward_cm_devices,
        n_outward_only_cm_devices,
        adas_all_features_allowed,
        severe_speeding_enabled,
        moderate_speeding_enabled,
        light_speeding_enabled,
        heavy_speeding_enabled,
        max_speed_in_cab_audio_alerts_disabled,
        crash_alert_disabled,
        harsh_driving_alert_disabled,
        speeding_in_cab_audio_alerts_enabled,
        speeding_kmph_over_limit_enabled,
        speeding_kmph_over_limit_threshold,
        speeding_percent_over_limit_enabled,
        speeding_percent_over_limit_threshold,
        severity_settings_speed_over_limit_unit AS severity_settings_speed_over_limit_unit_enum,
        CASE WHEN severity_settings_speed_over_limit_unit = 0 THEN 'invalid_unit'
             WHEN severity_settings_speed_over_limit_unit = 1 THEN 'percentage'
             WHEN severity_settings_speed_over_limit_unit = 2 THEN 'miles_per_hour'
             WHEN severity_settings_speed_over_limit_unit = 3 THEN 'kilometers_per_hour'
             WHEN severity_settings_speed_over_limit_unit = 4 THEN 'milliknots_per_hour' END AS severity_settings_speed_over_limit_unit,
        severe_speeding_speed_over_limit_threshold,
        severe_speeding_time_before_alert_ms,
        severe_speeding_send_to_safety_inbox,
        severe_speeding_auto_add_to_coaching,
        moderate_speeding_speed_over_limit_threshold,
        moderate_speeding_time_before_alert_ms,
        moderate_speeding_send_to_safety_inbox,
        moderate_speeding_auto_add_to_coaching,
        light_speeding_speed_over_limit_threshold,
        light_speeding_time_before_alert_ms,
        light_speeding_send_to_safety_inbox,
        light_speeding_auto_add_to_coaching,
        heavy_speeding_speed_over_limit_threshold,
        heavy_speeding_time_before_alert_ms,
        heavy_speeding_send_to_safety_inbox,
        heavy_speeding_auto_add_to_coaching,
        driverless_nudges_enabled,
        camera_recording_mode,
        CASE WHEN eligible_distracted_driving = TRUE
                AND adas_all_features_allowed = TRUE
                AND distracted_driving_detection_enabled = TRUE THEN TRUE ELSE FALSE END AS distracted_driving_detection_enabled,
        CASE WHEN eligible_distracted_driving = TRUE
                AND adas_all_features_allowed = TRUE
                AND distracted_driving_detection_enabled = TRUE
                AND distracted_driving_audio_alerts_enabled = TRUE THEN TRUE ELSE FALSE END AS distracted_driving_detection_audio_alerts_enabled,
        CASE WHEN eligible_distracted_driving = TRUE
                AND adas_all_features_allowed = TRUE
                AND distracted_driving_detection_enabled = TRUE
                AND distracted_driving_audio_alert_promotion_enabled = TRUE THEN TRUE ELSE FALSE END AS distracted_driving_audio_alert_promotion_enabled,
        CASE WHEN eligible_distracted_driving = TRUE
                AND adas_all_features_allowed = TRUE
                AND distracted_driving_detection_enabled = TRUE
                THEN distracted_driving_minimum_speed_enum ELSE NULL END AS distracted_driving_detection_minimum_speed_enum,
        CASE WHEN eligible_distracted_driving = TRUE
                AND adas_all_features_allowed = TRUE
                AND policy_violations_enabled = TRUE THEN TRUE ELSE FALSE END AS policy_violations_enabled,
        CASE WHEN eligible_distracted_driving = TRUE
                AND adas_all_features_allowed = TRUE
                AND policy_violations_enabled = TRUE
                AND policy_violation_audio_alerts_enabled = TRUE THEN TRUE ELSE FALSE END AS policy_violations_audio_alerts_enabled,
        CASE WHEN eligible_distracted_driving = TRUE
                AND adas_all_features_allowed = TRUE
                AND policy_violations_enabled = TRUE
                THEN policy_violation_minimum_speed ELSE NULL END AS policy_violations_minimum_speed_enum,
        CASE WHEN eligible_mobile_usage = TRUE
                AND adas_all_features_allowed = TRUE
                AND mobile_usage_enabled = TRUE THEN TRUE ELSE FALSE END AS mobile_usage_enabled,
        CASE WHEN eligible_mobile_usage = TRUE
                AND adas_all_features_allowed = TRUE
                AND mobile_usage_enabled = TRUE
                AND mobile_usage_audio_alerts_enabled = TRUE THEN TRUE ELSE FALSE END AS mobile_usage_audio_alerts_enabled,
        CASE WHEN eligible_mobile_usage = TRUE
                AND adas_all_features_allowed = TRUE
                AND mobile_usage_enabled = TRUE
                THEN mobile_usage_audio_alert_promotion_enabled ELSE FALSE END AS mobile_usage_audio_alert_promotion_enabled,
        CASE WHEN eligible_mobile_usage = TRUE
                AND adas_all_features_allowed = TRUE
                AND mobile_usage_enabled = TRUE
                AND mobile_usage_audio_alert_promotion_enabled = TRUE
                THEN mobile_usage_audio_alert_count_threshold ELSE NULL END AS mobile_usage_audio_alert_count_threshold,
        CASE WHEN eligible_mobile_usage = TRUE
                AND adas_all_features_allowed = TRUE
                AND mobile_usage_enabled = TRUE
                THEN mobile_usage_audio_alert_duration_ms ELSE NULL END AS mobile_usage_audio_alert_duration_ms,
        CASE WHEN eligible_mobile_usage = TRUE
                AND adas_all_features_allowed = TRUE
                AND mobile_usage_enabled = TRUE
                THEN mobile_usage_per_trip_enabled ELSE NULL END AS mobile_usage_per_trip_enabled,
        CASE WHEN eligible_mobile_usage = TRUE
                AND adas_all_features_allowed = TRUE
                AND mobile_usage_enabled = TRUE
                THEN mobile_usage_minimum_speed_enum ELSE NULL END AS mobile_usage_minimum_speed_enum,
        CASE WHEN eligible_seatbelt = TRUE
                AND adas_all_features_allowed = TRUE
                AND seatbelt_enabled = TRUE THEN TRUE ELSE FALSE END AS seatbelt_enabled,
        CASE WHEN eligible_seatbelt = TRUE
                AND adas_all_features_allowed = TRUE
                AND seatbelt_enabled = TRUE
                AND seatbelt_audio_alerts_enabled = TRUE THEN TRUE ELSE FALSE END AS seatbelt_audio_alerts_enabled,
        CASE WHEN eligible_seatbelt = TRUE
                AND adas_all_features_allowed = TRUE
                AND seatbelt_enabled = TRUE
                AND seatbelt_audio_alert_promotion_enabled = TRUE THEN TRUE ELSE FALSE END AS seatbelt_audio_alert_promotion_enabled,
        CASE WHEN eligible_seatbelt = TRUE
                AND adas_all_features_allowed = TRUE
                AND seatbelt_enabled = TRUE
                THEN seatbelt_minimum_speed_enum ELSE NULL END AS seatbelt_minimum_speed_enum,
        CASE WHEN eligible_rolling_stop = TRUE
                AND rolling_stop_enabled = TRUE THEN TRUE ELSE FALSE END AS rolling_stop_enabled,
        CASE WHEN eligible_rolling_stop = TRUE
                AND rolling_stop_enabled = TRUE THEN rolling_stop_threshold_milliknots ELSE NULL END AS rolling_stop_threshold_milliknots,
        in_cab_stop_sign_violation_enabled,
        in_cab_stop_sign_violation_audio_alerts_enabled,
        CASE WHEN eligible_following_distance = TRUE
                AND adas_all_features_allowed = TRUE
                AND following_distance_enabled = TRUE
                THEN TRUE ELSE FALSE END AS following_distance_enabled,
        CASE WHEN eligible_following_distance = TRUE
                AND adas_all_features_allowed = TRUE
                AND following_distance_enabled = TRUE
                AND following_distance_audio_alerts_enabled = TRUE THEN TRUE ELSE FALSE END AS following_distance_audio_alerts_enabled,
        CASE WHEN eligible_following_distance = TRUE
                AND adas_all_features_allowed = TRUE
                AND following_distance_enabled = TRUE
                AND following_distance_audio_alert_promotion_enabled = TRUE THEN TRUE ELSE FALSE END AS following_distance_audio_alert_promotion_enabled,
        CASE WHEN eligible_following_distance = TRUE
                AND adas_all_features_allowed = TRUE
                AND following_distance_enabled = TRUE
                THEN following_distance_minimum_speed_enum ELSE NULL END AS following_distance_minimum_speed_enum,
        CASE WHEN eligible_forward_collision_warning = TRUE
                AND adas_all_features_allowed = TRUE
                AND forward_collision_warning_enabled = TRUE THEN TRUE ELSE FALSE END AS forward_collision_warning_enabled,
        CASE WHEN eligible_forward_collision_warning = TRUE
                AND adas_all_features_allowed = TRUE
                AND forward_collision_warning_enabled = TRUE
                AND forward_collision_warning_audio_alerts_enabled = TRUE THEN TRUE ELSE FALSE END AS forward_collision_warning_audio_alerts_enabled,
        CASE WHEN eligible_lane_departure = TRUE
            AND adas_all_features_allowed = TRUE
            AND lane_departure_warning_enabled = TRUE THEN TRUE ELSE FALSE END AS lane_departure_warning_enabled,
        CASE WHEN eligible_lane_departure = TRUE
            AND adas_all_features_allowed = TRUE
            AND lane_departure_warning_enabled = TRUE
            AND lane_departure_warning_audio_alerts_enabled = TRUE THEN TRUE ELSE FALSE END AS lane_departure_warning_audio_alerts_enabled,
        CASE WHEN eligible_lane_departure = TRUE
            AND adas_all_features_allowed = TRUE
            AND lane_departure_warning_enabled = TRUE
            AND lane_departure_warning_audio_alert_promotion_enabled = TRUE THEN TRUE ELSE FALSE END AS lane_departure_warning_audio_alert_promotion_enabled,
        CASE WHEN eligible_lane_departure = TRUE
            AND adas_all_features_allowed = TRUE
            AND lane_departure_warning_enabled = TRUE
            THEN lane_departure_warning_minimum_speed_enum ELSE NULL END AS lane_departure_warning_minimum_speed_enum,
        CASE WHEN eligible_inward_obstruction = TRUE
                AND adas_all_features_allowed = TRUE
                AND inward_obstruction_enabled = TRUE THEN TRUE ELSE FALSE END AS inward_obstruction_enabled,
        CASE WHEN eligible_inward_obstruction = TRUE
                AND adas_all_features_allowed = TRUE
                AND inward_obstruction_enabled = TRUE
                AND inward_obstruction_audio_alerts_enabled = TRUE THEN TRUE ELSE FALSE END AS inward_obstruction_audio_alerts_enabled,
        CASE WHEN eligible_inward_obstruction = TRUE
                AND adas_all_features_allowed = TRUE
                AND inward_obstruction_enabled = TRUE
                AND inward_obstruction_audio_alert_promotion_enabled = TRUE THEN TRUE ELSE FALSE END AS inward_obstruction_audio_alert_promotion_enabled,
        CASE WHEN eligible_inward_obstruction = TRUE
                AND adas_all_features_allowed = TRUE
                AND inward_obstruction_enabled = TRUE
                THEN inward_obstruction_minimum_speed_enum ELSE NULL END AS inward_obstruction_minimum_speed_enum,
        CASE WHEN eligible_eating_drinking = TRUE
                AND adas_all_features_allowed = TRUE
                AND eating_drinking_enabled = TRUE THEN TRUE ELSE FALSE END AS eating_drinking_enabled,
        CASE WHEN eligible_eating_drinking = TRUE
                AND adas_all_features_allowed = TRUE
                AND eating_drinking_enabled = TRUE
                AND eating_drinking_audio_alerts_enabled = TRUE THEN TRUE ELSE FALSE END AS eating_drinking_audio_alerts_enabled,
        CASE WHEN eligible_smoking = TRUE
                AND adas_all_features_allowed = TRUE
                AND smoking_enabled = TRUE THEN TRUE ELSE FALSE END AS smoking_enabled,
        CASE WHEN eligible_smoking = TRUE
                AND adas_all_features_allowed = TRUE
                AND smoking_enabled = TRUE
                AND smoking_audio_alerts_enabled = TRUE THEN TRUE ELSE FALSE END AS smoking_audio_alerts_enabled,
        CASE WHEN eligible_mask = TRUE
                AND adas_all_features_allowed = TRUE
                AND mask_enabled = TRUE THEN TRUE ELSE FALSE END AS mask_enabled,
        CASE WHEN eligible_mask = TRUE
                AND adas_all_features_allowed = TRUE
                AND mask_enabled = TRUE
                AND mask_audio_alerts_enabled = TRUE THEN TRUE ELSE FALSE END AS mask_audio_alerts_enabled,
        CASE WHEN eligible_drowsiness = TRUE
                AND adas_all_features_allowed = TRUE
                AND drowsiness_detection_enabled = TRUE THEN TRUE ELSE FALSE END AS drowsiness_detection_enabled,
        CASE WHEN eligible_drowsiness = TRUE
                AND adas_all_features_allowed = TRUE
                AND drowsiness_detection_enabled = TRUE
                AND drowsiness_detection_audio_alerts_enabled = TRUE THEN TRUE ELSE FALSE END AS drowsiness_detection_audio_alerts_enabled,
            map(1, 25, --map of speed enum values to mph values, both 1 and 4 map to 25mph
                2, 5,
                3, 10,
                4, 25,
                5, 35,
                6, 45,
                7, 55) as speed_enum_to_mph_map,
            map(0, 25, --map of speed enum values to mph values, both 0 and 3 map to 25mph
                1, 5,
                2, 10,
                3, 25,
                4, 35) as policy_violation_speed_enum_to_mph_map,
         COALESCE(max_speed_enabled, FALSE) AS max_speed_enabled,
         max_speed_time_before_alert_ms,
         max_speed_threshold_milliknots,
         COALESCE(max_speed_send_to_safety_inbox, FALSE) AS max_speed_send_to_safety_inbox,
         COALESCE(max_speed_auto_add_to_coaching, FALSE) AS max_speed_auto_add_to_coaching,
         harsh_accel_passenger,
         harsh_brake_passenger,
         harsh_turn_passenger,
         harsh_accel_light_duty,
         harsh_brake_light_duty,
         harsh_turn_light_duty,
         harsh_accel_heavy_duty,
         harsh_brake_heavy_duty,
         harsh_turn_heavy_duty,
         COALESCE(csl_enabled, FALSE) AS csl_enabled
    from {source}.stg_safety_org_settings
    where date = '{PARTITION_START}')


    SELECT
    date,
    org_id,
    n_cm_devices,
    n_inward_cm_devices,
    n_outward_only_cm_devices,
    adas_all_features_allowed,
    severe_speeding_enabled,
    moderate_speeding_enabled,
    light_speeding_enabled,
    heavy_speeding_enabled,
    speeding_in_cab_audio_alerts_enabled,
    severity_settings_speed_over_limit_unit_enum,
    severity_settings_speed_over_limit_unit,
    severe_speeding_speed_over_limit_threshold,
    severe_speeding_time_before_alert_ms,
    COALESCE(severe_speeding_send_to_safety_inbox, FALSE) AS severe_speeding_send_to_safety_inbox,
    COALESCE(severe_speeding_auto_add_to_coaching, FALSE) AS severe_speeding_auto_add_to_coaching,
    moderate_speeding_speed_over_limit_threshold,
    moderate_speeding_time_before_alert_ms,
    COALESCE(moderate_speeding_send_to_safety_inbox, FALSE) AS moderate_speeding_send_to_safety_inbox,
    COALESCE(moderate_speeding_auto_add_to_coaching, FALSE) AS moderate_speeding_auto_add_to_coaching,
    light_speeding_speed_over_limit_threshold,
    light_speeding_time_before_alert_ms,
    COALESCE(light_speeding_send_to_safety_inbox, FALSE) AS light_speeding_send_to_safety_inbox,
    COALESCE(light_speeding_auto_add_to_coaching, FALSE) AS light_speeding_auto_add_to_coaching,
    heavy_speeding_speed_over_limit_threshold,
    heavy_speeding_time_before_alert_ms,
    COALESCE(heavy_speeding_send_to_safety_inbox, FALSE) AS heavy_speeding_send_to_safety_inbox,
    COALESCE(heavy_speeding_auto_add_to_coaching, FALSE) AS heavy_speeding_auto_add_to_coaching,
    CASE WHEN distracted_driving_detection_enabled = TRUE
            OR mobile_usage_enabled = TRUE
            OR drowsiness_detection_enabled = TRUE
            THEN TRUE ELSE FALSE END AS distracted_driving_detection_enabled,
    distracted_driving_detection_enabled AS inattentive_driving_enabled,
    distracted_driving_detection_audio_alerts_enabled AS inattentive_driving_audio_alerts_enabled,
    distracted_driving_audio_alert_promotion_enabled AS inattentive_driving_audio_alert_promotion_enabled,
    distracted_driving_detection_minimum_speed_enum AS inattentive_driving_minimum_speed_enum,
    policy_violations_enabled,
    policy_violations_audio_alerts_enabled,
    policy_violations_minimum_speed_enum,
    eating_drinking_enabled,
    eating_drinking_audio_alerts_enabled,
    smoking_enabled,
    smoking_audio_alerts_enabled,
    mask_enabled,
    mask_audio_alerts_enabled,
    CASE WHEN policy_violations_minimum_speed_enum > 0
        THEN policy_violation_speed_enum_to_mph_map[policy_violations_minimum_speed_enum] ELSE NULL END AS policy_violations_minimum_speed_mph,
    mobile_usage_enabled,
    mobile_usage_audio_alerts_enabled,
    COALESCE(mobile_usage_audio_alert_promotion_enabled, FALSE) AS mobile_usage_audio_alert_promotion_enabled,
    CASE WHEN mobile_usage_audio_alert_promotion_enabled
         THEN mobile_usage_audio_alert_count_threshold ELSE NULL
         END AS mobile_usage_audio_alert_count_threshold,
    mobile_usage_audio_alert_duration_ms,
    mobile_usage_per_trip_enabled,
    mobile_usage_minimum_speed_enum,
    CASE WHEN mobile_usage_minimum_speed_enum > 0
        THEN speed_enum_to_mph_map[mobile_usage_minimum_speed_enum] ELSE NULL END AS mobile_usage_minimum_speed_mph,
    seatbelt_enabled,
    seatbelt_audio_alerts_enabled,
    seatbelt_audio_alert_promotion_enabled,
    seatbelt_minimum_speed_enum,
    rolling_stop_enabled,
    rolling_stop_threshold_milliknots,
    in_cab_stop_sign_violation_enabled,
    in_cab_stop_sign_violation_audio_alerts_enabled,
    following_distance_enabled,
    following_distance_audio_alerts_enabled,
    following_distance_audio_alert_promotion_enabled,
    following_distance_minimum_speed_enum,
    forward_collision_warning_enabled,
    forward_collision_warning_audio_alerts_enabled,
    lane_departure_warning_enabled,
    lane_departure_warning_audio_alerts_enabled,
    lane_departure_warning_audio_alert_promotion_enabled,
    lane_departure_warning_minimum_speed_enum,
    inward_obstruction_enabled,
    inward_obstruction_audio_alerts_enabled,
    inward_obstruction_audio_alert_promotion_enabled,
    inward_obstruction_minimum_speed_enum,
    drowsiness_detection_enabled,
    drowsiness_detection_audio_alerts_enabled,
    max_speed_enabled,
    NOT max_speed_in_cab_audio_alerts_disabled AS max_speed_in_cab_audio_alerts_enabled,
    NOT crash_alert_disabled AS crash_in_cab_audio_alerts_enabled,
    NOT harsh_driving_alert_disabled AS harsh_driving_in_cab_audio_alerts_enabled,
    CASE WHEN
        harsh_accel_passenger = 4 AND harsh_accel_light_duty = 4 AND harsh_accel_heavy_duty = 4
        THEN FALSE
    ELSE TRUE END AS harsh_accel_enabled,
    CASE WHEN
        harsh_brake_passenger = 4 AND harsh_brake_light_duty = 4 AND harsh_brake_heavy_duty = 4
        THEN FALSE
    ELSE TRUE END AS harsh_brake_enabled,
    CASE WHEN
        harsh_turn_passenger = 4 AND harsh_turn_light_duty = 4 AND harsh_turn_heavy_duty = 4
        THEN FALSE
    ELSE TRUE END AS harsh_turn_enabled,
    harsh_accel_passenger,
    harsh_brake_passenger,
    harsh_turn_passenger,
    harsh_accel_light_duty,
    harsh_brake_light_duty,
    harsh_turn_light_duty,
    harsh_accel_heavy_duty,
    harsh_brake_heavy_duty,
    harsh_turn_heavy_duty,
    max_speed_time_before_alert_ms,
    max_speed_threshold_milliknots,
    max_speed_send_to_safety_inbox,
    max_speed_auto_add_to_coaching,
    csl_enabled,
    driverless_nudges_enabled,
    camera_recording_mode,
    speeding_kmph_over_limit_threshold,
    speeding_kmph_over_limit_enabled,
    speeding_percent_over_limit_threshold,
    speeding_percent_over_limit_enabled,
    -- In-cab speeding audio alerts enabled for each severity level
    -- Enabled if in_cab_threshold <= severity_threshold (in-cab fires at or before the severity event)
    -- Unit conversions: 1 mph = 1.60934 kmph, 1 milliknot = 0.001852 kmph
    CASE
        WHEN COALESCE(light_speeding_enabled, FALSE) = FALSE THEN FALSE
        WHEN COALESCE(speeding_in_cab_audio_alerts_enabled, FALSE) = FALSE THEN FALSE
        -- Both use percentage
        WHEN speeding_percent_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 1
            THEN speeding_percent_over_limit_threshold <= light_speeding_speed_over_limit_threshold
        -- In-cab uses kmph, severity uses kmph
        WHEN speeding_kmph_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 3
            THEN speeding_kmph_over_limit_threshold <= light_speeding_speed_over_limit_threshold
        -- In-cab uses kmph, severity uses mph (convert in-cab kmph to mph)
        WHEN speeding_kmph_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 2
            THEN ROUND(speeding_kmph_over_limit_threshold / 1.60934) <= light_speeding_speed_over_limit_threshold
        -- In-cab uses kmph, severity uses milliknots (convert severity milliknots to kmph)
        WHEN speeding_kmph_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 4
            THEN ROUND(speeding_kmph_over_limit_threshold) <= ROUND(light_speeding_speed_over_limit_threshold * 0.001852)
        ELSE FALSE
    END AS light_speeding_in_cab_audio_alerts_enabled,
    CASE
        WHEN COALESCE(moderate_speeding_enabled, FALSE) = FALSE THEN FALSE
        WHEN COALESCE(speeding_in_cab_audio_alerts_enabled, FALSE) = FALSE THEN FALSE
        -- Both use percentage
        WHEN speeding_percent_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 1
            THEN speeding_percent_over_limit_threshold <= moderate_speeding_speed_over_limit_threshold
        -- In-cab uses kmph, severity uses kmph
        WHEN speeding_kmph_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 3
            THEN speeding_kmph_over_limit_threshold <= moderate_speeding_speed_over_limit_threshold
        -- In-cab uses kmph, severity uses mph (convert in-cab kmph to mph)
        WHEN speeding_kmph_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 2
            THEN ROUND(speeding_kmph_over_limit_threshold / 1.60934) <= moderate_speeding_speed_over_limit_threshold
        -- In-cab uses kmph, severity uses milliknots (convert severity milliknots to kmph)
        WHEN speeding_kmph_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 4
            THEN ROUND(speeding_kmph_over_limit_threshold) <= ROUND(moderate_speeding_speed_over_limit_threshold * 0.001852)
        ELSE FALSE
    END AS moderate_speeding_in_cab_audio_alerts_enabled,
    CASE
        WHEN COALESCE(heavy_speeding_enabled, FALSE) = FALSE THEN FALSE
        WHEN COALESCE(speeding_in_cab_audio_alerts_enabled, FALSE) = FALSE THEN FALSE
        -- Both use percentage
        WHEN speeding_percent_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 1
            THEN speeding_percent_over_limit_threshold <= heavy_speeding_speed_over_limit_threshold
        -- In-cab uses kmph, severity uses kmph
        WHEN speeding_kmph_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 3
            THEN speeding_kmph_over_limit_threshold <= heavy_speeding_speed_over_limit_threshold
        -- In-cab uses kmph, severity uses mph (convert in-cab kmph to mph)
        WHEN speeding_kmph_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 2
            THEN ROUND(speeding_kmph_over_limit_threshold / 1.60934) <= heavy_speeding_speed_over_limit_threshold
        -- In-cab uses kmph, severity uses milliknots (convert severity milliknots to kmph)
        WHEN speeding_kmph_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 4
            THEN ROUND(speeding_kmph_over_limit_threshold) <= ROUND(heavy_speeding_speed_over_limit_threshold * 0.001852)
        ELSE FALSE
    END AS heavy_speeding_in_cab_audio_alerts_enabled,
    CASE
        WHEN COALESCE(severe_speeding_enabled, FALSE) = FALSE THEN FALSE
        WHEN COALESCE(speeding_in_cab_audio_alerts_enabled, FALSE) = FALSE THEN FALSE
        -- Both use percentage
        WHEN speeding_percent_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 1
            THEN speeding_percent_over_limit_threshold <= severe_speeding_speed_over_limit_threshold
        -- In-cab uses kmph, severity uses kmph
        WHEN speeding_kmph_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 3
            THEN speeding_kmph_over_limit_threshold <= severe_speeding_speed_over_limit_threshold
        -- In-cab uses kmph, severity uses mph (convert in-cab kmph to mph)
        WHEN speeding_kmph_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 2
            THEN ROUND(speeding_kmph_over_limit_threshold / 1.60934) <= severe_speeding_speed_over_limit_threshold
        -- In-cab uses kmph, severity uses milliknots (convert severity milliknots to kmph)
        WHEN speeding_kmph_over_limit_enabled = TRUE AND severity_settings_speed_over_limit_unit_enum = 4
            THEN ROUND(speeding_kmph_over_limit_threshold) <= ROUND(severe_speeding_speed_over_limit_threshold * 0.001852)
        ELSE FALSE
    END AS severe_speeding_in_cab_audio_alerts_enabled
    FROM base
"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""Organization level safety settings with logic applied to limit safety settings to those organizations with compatible hardware.""",
        row_meaning="""One organization's safety settings as of date.""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=["date", "org_id"],
    upstreams=["product_analytics_staging.stg_safety_org_settings"],
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_dim_organizations_safety_settings"),
        NonNullDQCheck(
            name="dq_non_null_dim_organizations_safety_settings",
            non_null_columns=["org_id", "mobile_usage_enabled"],
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_dim_organizations_safety_settings",
            primary_keys=["date", "org_id"],
            block_before_write=True,
        ),
    ],
)
def dim_organizations_safety_settings(context: AssetExecutionContext) -> str:
    context.log.info("Updating dim_organizations_safety_settings")
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
