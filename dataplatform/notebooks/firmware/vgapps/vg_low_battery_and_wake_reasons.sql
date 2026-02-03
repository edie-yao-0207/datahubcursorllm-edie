-- Databricks notebook source
create or replace temp view device_daily_low_battery_counts as (
  select
    date,
    org_id,
    object_id as device_id,
    count(*) as low_battery_count
  from kinesisstats.osdpowerstate
  where
    date between date_sub(current_date(), 8) and current_date()
    and org_id != 1
    -- we only want to look at transitions from moderate to low power which were reported
    -- with duration reason low vehicle battery.  Other transitions (such as to and from
    -- brief wake) also get reported with the low vehicle battery duration reason, and
    -- including those would double-count the low battery events.
    and value.proto_value.power_state_info.duration_reason = 2  -- low vehicle battery
    and value.proto_value.power_state_info.current_power_state = 3  -- in low power
    and value.proto_value.power_state_info.last_power_state = 6 -- from moderate power
  group by
    date,
    org_id,
    object_id
)

-- COMMAND ----------

create or replace temp view device_daily_wake_counts as (
  select
    date,
    org_id,
    object_id as device_id,
    sum(case when value.proto_value.power_state_info.change_reason = 3 then 1 else 0 end) as motion_wake_count,
    sum(case when value.proto_value.power_state_info.change_reason = 5 then 1 else 0 end) as voltage_wake_count
  from kinesisstats.osdpowerstate
  where
    date between date_sub(current_date(), 8) and current_date()
    and org_id != 1
  group by
    date,
    org_id,
    object_id
)

-- COMMAND ----------

create or replace temp view device_daily_joined as (
  select
    a.date,
    a.org_id,
    a.org_type,
    a.device_id,
    a.product_type,
    a.latest_build_on_day as build,
    a.rollout_stage_id,
    a.product_program_id,
    coalesce(b.low_battery_count, 0) as low_battery_count,
    coalesce(c.motion_wake_count, 0) as motion_wake_count,
    coalesce(c.voltage_wake_count, 0) as voltage_jump_wake_count
  from data_analytics.dataprep_vg3x_daily_health_metrics as a
  left join device_daily_low_battery_counts as b
    on a.date = b.date
    and a.org_id = b.org_id
    and a.device_id = b.device_id
  left join device_daily_wake_counts as c
    on a.date = c.date
    and a.org_id = c.org_id
    and a.device_id = c.device_id
  where
    a.date between date_sub(current_date(), 8) and current_date()
    and a.org_id != 1
)

-- COMMAND ----------

create table if not exists firmware.vg_low_battery_and_wake_reasons
using delta
partitioned by (date)
select * from device_daily_joined

-- COMMAND ----------

merge into firmware.vg_low_battery_and_wake_reasons as target
using device_daily_joined as updates
on target.date = updates.date
and target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.product_type = updates.product_type
when matched then update set *
when not matched then insert * ;

ALTER TABLE firmware.vg_low_battery_and_wake_reasons
SET TBLPROPERTIES ('comment' = 'This reports low battery events and wake reasons for VGs for a given day.');

ALTER TABLE firmware.vg_low_battery_and_wake_reasons
CHANGE date
COMMENT 'The date that we are reporting battery and voltage jump information for.';

ALTER TABLE firmware.vg_low_battery_and_wake_reasons
CHANGE org_id
COMMENT 'The Samsara Cloud dashboard ID that the data belongs to';

ALTER TABLE firmware.vg_low_battery_and_wake_reasons
CHANGE org_type
COMMENT 'The organization type that these devices belong to. One of Internal, or Customer.';

ALTER TABLE firmware.vg_low_battery_and_wake_reasons
CHANGE device_id
COMMENT 'The device ID of the VG that we are reporting metrics for';

ALTER TABLE firmware.vg_low_battery_and_wake_reasons
CHANGE product_type
COMMENT 'The product type of the VG';

ALTER TABLE firmware.vg_low_battery_and_wake_reasons
CHANGE build
COMMENT 'The firmware build running on the VG';

ALTER TABLE firmware.vg_low_battery_and_wake_reasons
CHANGE rollout_stage_id
COMMENT 'The firmware rollout stage of the VG';

ALTER TABLE firmware.vg_low_battery_and_wake_reasons
CHANGE product_program_id
COMMENT 'The product program ID for the firmware rollout for the VG';

ALTER TABLE firmware.vg_low_battery_and_wake_reasons
CHANGE low_battery_count
COMMENT 'The number of low battery events reported by the VG that day';

ALTER TABLE firmware.vg_low_battery_and_wake_reasons
CHANGE motion_wake_count
COMMENT 'The number of wake events from accel motion for the day';

ALTER TABLE firmware.vg_low_battery_and_wake_reasons
CHANGE voltage_jump_wake_count
COMMENT 'The number of wake events from voltage jumps for the day';
