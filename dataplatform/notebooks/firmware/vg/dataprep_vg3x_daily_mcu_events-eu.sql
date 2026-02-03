-- Databricks notebook source
-- This objectStat is only recorded by VG3x with Modi or Thor devices 
CREATE OR REPLACE TEMP VIEW daily_device_mcu_fatal_counts AS (
   SELECT 
      a.date,
      a.org_id,
      a.org_type,
      a.org_name,
      a.device_id,
      a.product_type,
      a.latest_build_on_day,
      a.can_bus_type,
      a.has_modi,
      a.status,
      a.gw_first_heartbeat_date,
      a.product_version,
      a.rollout_stage_id,
      a.product_program_id,
      a.product_program_id_type,
      b.value.proto_value.vg_mcu_fatal.mcu_fw_version,
      b.value.proto_value.vg_mcu_fatal.reason,
      count(b.value.proto_value.vg_mcu_fatal.reason) AS count
    FROM data_analytics.dataprep_vg3x_daily_health_metrics AS a
    LEFT JOIN kinesisstats.osDVgMcuFatal AS b ON
      a.date = b.date
      AND a.device_id = b.object_id
      AND a.org_id = b.org_id
      AND b.value.is_databreak = 'false' 
      AND b.value.is_end = 'false'
    WHERE a.has_modi = 'true' 
      or a.product_type like "%VG5%"
    GROUP BY
      a.date,
      a.org_id,
      a.org_type,
      a.org_name,
      a.device_id,
      a.product_type,
      a.latest_build_on_day,
      a.can_bus_type,
      a.has_modi,
      a.status,
      a.gw_first_heartbeat_date,
      a.product_version,
      a.rollout_stage_id,
      a.product_program_id,
      a.product_program_id_type,
      b.value.proto_value.vg_mcu_fatal.mcu_fw_version,
      b.value.proto_value.vg_mcu_fatal.reason
  );

-- COMMAND ----------

create table if not exists data_analytics.dataprep_vg3x_daily_mcu_events
using delta
partitioned by (date)
as
select * from daily_device_mcu_fatal_counts

-- COMMAND ----------

--update calculated fields for the last seven days
create or replace temp view daily_device_mcu_fatal_counts_updates as (
  select *
  from daily_device_mcu_fatal_counts
  where date > date_sub(current_date(),7)
);

merge into data_analytics.dataprep_vg3x_daily_mcu_events as target
using daily_device_mcu_fatal_counts_updates as updates
on target.date = updates.date 
and target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.mcu_fw_version = updates.mcu_fw_version
and target.reason = updates.reason
when matched then update set *
when not matched then insert *

-- COMMAND ----------


