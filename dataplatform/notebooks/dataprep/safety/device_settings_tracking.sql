-- Databricks notebook source
create or replace temporary view current_device_settings_tracking as (
    select
     current_date() as device_settings_date,
      id,
     org_id,
     created_at,
     updated_at,
     device_settings_proto.distracted_driving_detection_safety_inbox_device_enabled,
     device_settings_proto.distracted_driving_detection_audio_alerts_device_enabled,
     device_settings_proto.following_distance_safety_inbox_device_enabled,
     device_settings_proto.following_distance_audio_alerts_device_enabled,
     device_settings_proto.forward_collision_warning_safety_inbox_device_enabled,
     device_settings_proto.forward_collision_warning_audio_alerts_device_enabled,
     device_settings_proto.forward_collision_warning_roi_config,
     device_settings_proto.camera_height_meters
   from productsdb.devices
);

-- COMMAND ----------

merge into dataprep_safety.device_settings_tracking as target
using current_device_settings_tracking as updates
on target.id = updates.id
and target.device_settings_date = updates.device_settings_date
when matched then update set *
when not matched then insert *
