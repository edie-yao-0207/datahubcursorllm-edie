-- Databricks notebook source
--Get wake reasons that should result from an engine ON state
create or replace temp view wake_reasons as (
  select
    date,
    org_id,
    object_id as device_id,
    value.proto_value.power_state_info.change_reason as wake_reason
  from kinesisstats.osdpowerstate
  where 
    value.proto_value.power_state_info.change_reason IN (0, 2, 3, 4, 5)
    and value.is_end = 'false'
    and value.is_databreak = 'false'
);

--Power State Enum  
-- 0  then "UNKNOWN"
-- 1  then "TIME"
-- 2  then "ENGINE_ON"
-- 3  then "ACCEL_MOVEMENT"
-- 4  then "GPS_MOVEMENT"
-- 5  then "VOLTAGE_JUMP"
-- 6  then "SLEEP_DISABLED"
-- 7  then "VIDEO_RETRIEVAL"
-- 8  then "BOOT"
-- 9  then "LOW_VEHICLE_BATTERY"
-- 10 then "EXTERNAL_POWER_CHANGE"
-- 11 then "OBD_MOVEMENT"
-- 12 then "UPGRADING"
-- 13 then "PANIC_BUTTON"
-- 14 then "SMARTCARD_READER"
-- 15 then "NO_GPS"
-- 16 then "AP_CLIENTS"

-- COMMAND ----------

--Get total number of wakes by wake reason
create or replace temp view vg_daily_wake_count as (
  select
    a.date,
    a.org_id,
    a.device_id,
    a.wake_reason,
    count(a.wake_reason) as wake_count
  from wake_reasons as a
  join data_analytics.dataprep_vg3x_daily_health_metrics as b on
    a.date = b.date
    and a.org_id = b.org_id
    and a.device_id = b.device_id
  group by 
    a.date,
    a.org_id,
    a.device_id,
    a.wake_reason
)

-- COMMAND ----------

--Join wake counts onto device level summary table
create or replace temp view device_wake_metrics as (
  select
    a.date,
    a.org_id,
    a.org_type,
    a.org_name,
    a.device_id,
    a.product_type,
    a.product_version,
    a.rollout_stage_id,
    a.product_program_id,
    a.product_program_id_type,
    a.latest_build_on_day,
    a.status,
    a.trip_count,
    a.can_bus_type,
    a.cable_type,
    a.has_modi,
    coalesce(b.wake_reason, 'null') as wake_reason,
    coalesce(b.wake_count, 0) as wake_count
  from data_analytics.dataprep_vg3x_daily_health_metrics as a
  left join vg_daily_wake_count as b on
    a.date = b.date
    and a.org_id = b.org_id
    and a.device_id = b.device_id
)

-- COMMAND ----------

--Daily level aggregation for tableau
create or replace temp view device_wake_metrics_agg as (
  select
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    latest_build_on_day,
    status,
    trip_count,
    can_bus_type,
    has_modi,
    wake_reason,
    sum(wake_count) as wake_count,
    sum(trip_count) as total_trips,
    count(distinct device_id) as total_devices
  from device_wake_metrics
  group by
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    latest_build_on_day,
    status,
    trip_count,
    can_bus_type,
    has_modi,
    wake_reason
    
)

-- COMMAND ----------

create table if not exists data_analytics.vg_wake_reason_distributions using delta
select * from device_wake_metrics_agg

-- COMMAND ----------

-- When trying to use the MERGE UPSERT method, I was running into an issue where duplicate rows would be
-- added. To mitigate this, I'm opted for deleting and rewriting the last seven days worth of rows to ensure
-- I don't have any duplicates that are being added.
create or replace temp view device_wake_metrics_updates as (
 select * from device_wake_metrics_agg
  where date >= date_sub(current_date(), 7)
);

delete from data_analytics.vg_wake_reason_distributions
where date >= date_sub(current_date(), 7);
  
merge into data_analytics.vg_wake_reason_distributions as target 
using device_wake_metrics_updates as updates 
on target.date = updates.date
when matched then update set *
when not matched then insert *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Appendix: EDA

-- COMMAND ----------

-- --Visualize daily aggregate data
-- with total_aggs as (
--   select
--     date,
--     sum(total_wakes) as total_wakes
--   from playground.wake_metrics_test
--   group by
--     date
-- ),

-- count_aggs as (
--   select
--   date,
--   wake_reason,
--   sum(wake_count) as wake_count
--   from playground.wake_metrics_test
--   group by 
--     date,
--     wake_reason
-- )

-- select
-- a.date,
-- b.wake_reason,
-- b.wake_count,
-- a.total_wakes,
-- b.wake_count / a.total_wakes as ratio
-- from total_aggs as a
-- join count_aggs as b on
-- a.date = b.date

-- COMMAND ----------

-- --Aggregate at daily level
-- create or replace temp view vg_daily_wake_metrics as (
--   select
--     date,
--     org_id,
--     org_type,
--     org_name,
--     product_type,
--     latest_build_on_day,
--     status,
--     trip_count,
--     can_bus_type,
--     gw_first_heartbeat_date,
--     wake_reason,
--     sum(wake_count) as wake_count,
--     sum(total_wakes) as total_wakes,
--     sum(trip_count) as total_trips
--   from device_wake_metrics
--   group by
--     date,
--     org_id,
--     org_type,
--     org_name,
--     product_type,
--     latest_build_on_day,
--     status,
--     trip_count,
--     can_bus_type,
--     gw_first_heartbeat_date,
--     wake_reason
-- )
