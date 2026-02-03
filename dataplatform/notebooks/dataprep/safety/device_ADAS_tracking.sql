-- Databricks notebook source
-- Enable upsert to modify the existing schema to add new columns
-- Revert to false after table has been modified
-- SET spark.databricks.delta.schema.autoMerge.enabled = true;

-- COMMAND ----------

create or replace temporary view device_settings_pid as (
  select
  device_settings_tracking.id as device_id,
  device_settings_tracking.org_id,
  device_settings_tracking.device_settings_date as date,
  first(device_settings_tracking.distracted_driving_detection_safety_inbox_device_enabled) as distracted_driving_detection_safety_inbox_device_enabled,
  first(device_settings_tracking.following_distance_safety_inbox_device_enabled) as following_distance_safety_inbox_device_enabled,
  first(device_settings_tracking.forward_collision_warning_safety_inbox_device_enabled) as forward_collision_warning_safety_inbox_device_enabled,
  cm_linked_vgs.cm_product_id
from dataprep_safety.device_settings_tracking device_settings_tracking
  join dataprep_safety.cm_linked_vgs cm_linked_vgs on
      device_settings_tracking.org_id = cm_linked_vgs.org_id and
    device_settings_tracking.id = cm_linked_vgs.linked_cm_id
group by
  device_settings_tracking.id,
  device_settings_tracking.org_id,
  device_settings_tracking.device_settings_date,
  cm_linked_vgs.cm_product_id
);

-- COMMAND ----------
----Updated date stop to backfill missing information from sequential job failues in November
-- cm_product_id = 43 corresponds to cm32
create or replace temporary view device_adas_settings as (
  select
  device_settings_pid.device_id,
  device_settings_pid.org_id,
  device_settings_pid.date,
  case when device_settings_pid.cm_product_id = 43 and (device_settings_pid.distracted_driving_detection_safety_inbox_device_enabled = true or org_adas_settings_tracking.ddd_enabled = true) then true else false end as ddd_enabled,
  case when device_settings_pid.cm_product_id = 43 and (device_settings_pid.following_distance_safety_inbox_device_enabled = true or org_adas_settings_tracking.fd_enabled = true) then true else false end as fd_enabled,
  case when device_settings_pid.cm_product_id = 43 and (device_settings_pid.forward_collision_warning_safety_inbox_device_enabled = true or org_adas_settings_tracking.fcw_enabled) = true then true else false end as fcw_enabled,
  CASE WHEN device_settings_pid.cm_product_id = 43 AND (org_adas_settings_tracking.seatbelt_enabled = true) THEN true ELSE false END AS seatbelt_enabled,
  CASE WHEN device_settings_pid.cm_product_id = 43 AND (org_adas_settings_tracking.phone_usage_enabled = true) THEN true ELSE false END AS phone_usage_enabled,
  CASE WHEN device_settings_pid.cm_product_id = 43 AND (org_adas_settings_tracking.inward_obstruction_enabled = true) THEN true ELSE false END AS inward_obstruction_enabled,
  CASE WHEN device_settings_pid.cm_product_id = 43 AND (org_adas_settings_tracking.mask_enabled = true) THEN true ELSE false END AS mask_enabled,
  CASE WHEN device_settings_pid.cm_product_id = 43 AND (org_adas_settings_tracking.stop_sign_enabled = true) THEN true ELSE false END AS stop_sign_enabled,
  CASE WHEN device_settings_pid.cm_product_id = 43 AND (org_adas_settings_tracking.railroad_enabled = true) THEN true ELSE false END AS railroad_enabled,
  CASE WHEN device_settings_pid.cm_product_id = 43 AND (org_adas_settings_tracking.speeding_enabled = true) THEN true ELSE false END AS speeding_enabled
from device_settings_pid device_settings_pid
  join dataprep_safety.org_adas_settings_tracking org_adas_settings_tracking on
      device_settings_pid.date = org_adas_settings_tracking.adas_date and
    device_settings_pid.org_id = org_adas_settings_tracking.id
  where
    device_settings_pid.date >= date_sub(current_date(),5)
);

-- COMMAND ----------

merge into dataprep_safety.device_adas_settings_tracking as target
using device_adas_settings as updates
on target.device_id = updates.device_id
  and target.org_id = updates.org_id
  and target.date = updates.date
when matched then update set *
when not matched then insert *;

ALTER TABLE dataprep_safety.device_adas_settings_tracking
SET TBLPROPERTIES ('comment' = 'Tracks the state of ADAS settings for a device on a particular date.');

ALTER TABLE dataprep_safety.device_adas_settings_tracking
CHANGE org_id
COMMENT 'The Samsara Cloud dashboard ID that the data belongs to';

ALTER TABLE dataprep_safety.device_adas_settings_tracking
CHANGE date
COMMENT 'The Samsara Cloud dashboard ID that the data belongs to';

ALTER TABLE dataprep_safety.device_adas_settings_tracking
CHANGE ddd_enabled
COMMENT 'Whether Distracted Driver Detection is enabled.';

ALTER TABLE dataprep_safety.device_adas_settings_tracking
CHANGE fd_enabled
COMMENT 'Whether Following Distance setting is enabled.';

ALTER TABLE dataprep_safety.device_adas_settings_tracking
CHANGE fcw_enabled
COMMENT 'Whether the Forward Collision Warning setting is enabled.';
