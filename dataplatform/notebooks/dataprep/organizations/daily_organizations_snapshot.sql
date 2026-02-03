SET spark.databricks.delta.schema.autoMerge.enabled = TRUE;
CREATE OR REPLACE TEMP VIEW todays_orgs AS (
  SELECT
    DATE_SUB(CURRENT_DATE(), 1) AS date,
    id,
    name,
    created_at,
    updated_at,
    auto_add_user_domain,
    temp_units,
    join_fleet_token,
    logo_type,
    logo_domain,
    logo_s3_location,
    login_domain,
    voice_coaching_enabled,
    voice_coaching_speed_threshold_milliknots,
    gamification_enabled,
    quarantine_enabled,
    locale,
    unit_system_override,
    language_override,
    currency_override,
    quarantine_enabled_at,
    vibration_units,
    message_push_notifications_enabled,
    audio_recording_enabled,
    trailer_selection_enabled,
    hotspot_unblock_deadline,
    wifi_hotspot_data_cap_bytes_per_gateway,
    message_text_to_speech_enabled,
    wake_on_motion_enabled,
    rollout_stage,
    driver_dispatch_manual_events_enabled,
    driver_route_self_assignment_enabled,
    settings_proto,
    driver_facing_trip_images_enabled,
    rolling_stop_threshold_milliknots,
    rolling_stop_enabled,
    driver_trailer_creation_enabled,
    max_num_of_trailers_selected,
    internal_type,
    is_early_adopter,
    release_type_enum
FROM clouddb.organizations TIMESTAMP AS OF DATE_SUB(CURRENT_DATE(), 1)
);

CREATE TABLE IF NOT EXISTS dataprep.daily_organizations_snapshot USING delta PARTITIONED BY (date)
COMMENT "DEPRECATED - please migrate to datamodel_core.dim_organizations. Daily snapshot of clouddb.organizations"
AS (
  SELECT
    date,
    id,
    name,
    created_at,
    updated_at,
    auto_add_user_domain,
    temp_units,
    join_fleet_token,
    logo_type,
    logo_domain,
    logo_s3_location,
    login_domain,
    voice_coaching_enabled,
    voice_coaching_speed_threshold_milliknots,
    gamification_enabled,
    quarantine_enabled,
    locale,
    unit_system_override,
    language_override,
    currency_override,
    quarantine_enabled_at,
    vibration_units,
    message_push_notifications_enabled,
    audio_recording_enabled,
    trailer_selection_enabled,
    hotspot_unblock_deadline,
    wifi_hotspot_data_cap_bytes_per_gateway,
    message_text_to_speech_enabled,
    wake_on_motion_enabled,
    rollout_stage,
    driver_dispatch_manual_events_enabled,
    driver_route_self_assignment_enabled,
    settings_proto,
    driver_facing_trip_images_enabled,
    rolling_stop_threshold_milliknots,
    rolling_stop_enabled,
    driver_trailer_creation_enabled,
    max_num_of_trailers_selected,
    internal_type,
    is_early_adopter,
    release_type_enum
  FROM todays_orgs
);

MERGE INTO dataprep.daily_organizations_snapshot AS target
USING todays_orgs AS updates
ON target.date = updates.date
  AND target.id = updates.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
