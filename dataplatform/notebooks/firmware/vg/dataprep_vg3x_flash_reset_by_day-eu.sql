-- Databricks notebook source
create or replace temp view vg_daily_flash_reset_devices as (
  select
    date,
    org_type,
    org_name,
    has_modi,
    org_id,
    product_type,
    product_version,
    product_program_id,
    product_program_id_type,
    rollout_stage_id,
    latest_build_on_day,
    can_bus_type,
    status,
    flash_status,
    count(distinct device_id) as device_count
  from data_analytics.fleetfwhealth_vg34_flash_devices
  group by
    date,
    org_type,
    org_name,
    has_modi,
    org_id,
    product_type,
    product_version,
    product_program_id,
    product_program_id_type,
    rollout_stage_id,
    latest_build_on_day,
    can_bus_type,
    status,
    flash_status
)

-- COMMAND ----------

create table if not exists data_analytics.dataprep_vg3x_flash_reset_by_day using delta
partitioned by (date)
select * from vg_daily_flash_reset_devices

-- COMMAND ----------

-- When trying to use the MERGE UPSERT method, I was running into an issue where duplicate rows would be
-- added. To mitigate this, I'm opted for deleting and rewriting the last seven days worth of rows to ensure
-- I don't have any duplicates that are being added.
create or replace temp view vg3x_metrics_daily_flash_reset_updates as (
 select * from vg_daily_flash_reset_devices
  where date >= date_sub(current_date(), 7)
);


delete from data_analytics.dataprep_vg3x_flash_reset_by_day
where date >= date_sub(current_date(), 7);
  
merge into data_analytics.dataprep_vg3x_flash_reset_by_day as target 
using vg3x_metrics_daily_flash_reset_updates as updates 
on target.date = updates.date
when matched then update set *
when not matched then insert *
