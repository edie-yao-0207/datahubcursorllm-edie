-- Databricks notebook source
create or replace temp view osdbattery as (
  select
    date,
    org_id,
    object_id,
    count(*) as voltage_count
  from kinesisstats.osdbattery
  where value.is_end = false
    and value.is_databreak = false
    and value.int_value > 0
    and value.int_value <= 3000
  group by 
    date,
    org_id,
    object_id
)

-- COMMAND ----------

-- Assume that internal battery is truly dead on a given day if
-- it records more than 10 battery voltages below 3V.
create or replace temp view dead_batteries as (
  select
    date,
    org_id,
    object_id,
    'Compromised' as internal_battery_status
  from osdbattery
  where voltage_count > 10
)

-- COMMAND ----------

create or replace temp view vg3x_batteries_joined as (
 select
    a.date,
    a.org_id,
    a.org_name,
    a.org_type,
    a.device_id,
    a.can_bus_type,
    a.product_type,
    a.status,
    a.latest_build_on_day,
    a.has_modi,
    a.product_version,
    a.product_program_id,
    a.product_program_id_type,
    a.rollout_stage_id,
    coalesce(b.internal_battery_status, 'Healthy') as internal_battery_status
   from data_analytics.dataprep_vg3x_daily_health_metrics as a
   left join dead_batteries as b on
     a.date = b.date 
     and a.org_id = b.org_id
     and a.device_id = b.object_id
);

-- COMMAND ----------

create or replace temp view vg3x_daily_aggregates as (
  select 
    date,
    org_id,
    org_name,
    org_type,
    can_bus_type,
    product_type,
    product_version,
    product_program_id,
    product_program_id_type,
    status,
    latest_build_on_day,
    has_modi,
    internal_battery_status,
    rollout_stage_id,
    count(distinct device_id) as device_count
  from vg3x_batteries_joined
  group by 
    date,
    org_id,
    org_name,
    org_type,
    can_bus_type,
    product_type,
    product_version,
    product_program_id,
    product_program_id_type,
    status,
    latest_build_on_day,
    has_modi,
    internal_battery_status,
    rollout_stage_id
)

-- COMMAND ----------

create table if not exists data_analytics.dataprep_vg_daily_internal_batteries
using delta
partitioned by (date)
select * from vg3x_daily_aggregates

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_battery_status_updates AS (
  SELECT * FROM vg3x_daily_aggregates 
  WHERE date > date_sub(current_date(),10)
);

MERGE INTO data_analytics.dataprep_vg_daily_internal_batteries AS target
USING device_battery_status_updates AS updates ON
target.date = updates.date
AND target.org_id = updates.org_id
AND target.org_name = updates.org_name
AND target.org_type = updates.org_type
AND target.can_bus_type = updates.can_bus_type
AND target.product_type = updates.product_type
AND target.product_version = updates.product_version
AND target.product_program_id = updates.product_program_id
AND target.product_program_id_type = updates.product_program_id_type
AND target.status = updates.status
AND target.latest_build_on_day = updates.latest_build_on_day
AND target.has_modi = updates.has_modi
AND target.internal_battery_status = updates.internal_battery_status
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------


