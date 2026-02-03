-- Databricks notebook source
-- Enable upsert to modify the existing schema to add new columns
-- Revert to false after table has been modified
-- SET spark.databricks.delta.schema.autoMerge.enabled = true;

-- COMMAND ----------

create or replace temporary view current_adas_settings_tracking as (
 select
 current_date() as adas_date,
 id,
 name,
 created_at,
 updated_at,
 locale,
 rollout_stage,
 settings_proto.adas_all_features_allowed as adas_all_features_allowed,
 settings_proto.forward_collision_warning_enabled as fcw_enabled,
 settings_proto.forward_collision_warning_audio_alerts_enabled as fcw_audio_alerts,
 settings_proto.forward_collision_sensitivity_level as fcw_sensitivity_level,
 settings_proto.following_distance_enabled as fd_enabled,
 settings_proto.following_distance_audio_alerts_enabled as fd_audio_alerts,
 settings_proto.distracted_driving_detection_enabled as ddd_enabled,
 settings_proto.distracted_driving_audio_alerts_enabled as ddd_audio_alerts,
 settings_proto.distracted_driving_sensitivity_level as ddd_sensitivity_level,
 settings_proto.safety.policy_violation_settings.seatbelt_enabled AS seatbelt_enabled,
 settings_proto.safety.policy_violation_settings.audio_alert_settings.seatbelt_enabled AS seatbelt_audio_alerts,
 settings_proto.safety.policy_violation_settings.phone_usage_enabled AS phone_usage_enabled,
 settings_proto.safety.policy_violation_settings.audio_alert_settings.phone_usage_enabled AS phone_usage_alerts,
 settings_proto.safety.policy_violation_settings.inward_obstruction_enabled AS inward_obstruction_enabled,
 settings_proto.safety.policy_violation_settings.audio_alert_settings.inward_obstruction_enabled AS inward_obstruction_alerts,
 settings_proto.safety.policy_violation_settings.mask_enabled AS mask_enabled,
 settings_proto.safety.policy_violation_settings.audio_alert_settings.mask_enabled AS mask_alerts,
 settings_proto.safety.in_cab_stop_sign_violation_alert_settings.enabled AS stop_sign_enabled,
 settings_proto.safety.in_cab_stop_sign_violation_alert_settings.enable_audio_alerts AS stop_sign_alerts,
 settings_proto.safety.in_cab_railroad_crossing_violation_alert_settings.enabled AS railroad_enabled,
 settings_proto.safety.in_cab_railroad_crossing_violation_alert_settings.enable_audio_alerts AS railroad_alerts,
 settings_proto.safety.speeding_settings.max_speed_setting.enabled AS speeding_enabled,
 settings_proto.safety.speeding_settings.max_speed_setting.in_cab_audio_alerts_enabled AS speeding_alerts
 from clouddb.organizations
);

-- COMMAND ----------

merge into dataprep_safety.org_adas_settings_tracking as target
using current_adas_settings_tracking as updates
on target.id = updates.id
and target.adas_date = updates.adas_date
when matched then update set *
when not matched then insert *;
