-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW vg_daily_flash_reset_devices AS (
  SELECT
    date,
    org_type,
    org_name,
    has_modi,
    has_baxter,
    org_id,
    product_type,
    latest_build_on_day,
    COALESCE(can_bus_type, 'null') AS can_bus_type,
    status,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    flash_status,
    count(distinct device_id) AS device_count
  FROM data_analytics.fleetfwhealth_vg34_flash_devices
  GROUP BY
    date,
    org_type,
    org_name,
    has_modi,
    has_baxter,
    org_id,
    product_type,
    latest_build_on_day,
    can_bus_type,
    status,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    flash_status
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.dataprep_vg3x_flash_reset_by_day USING DELTA
PARTITIONED BY (date)
SELECT * FROM vg_daily_flash_reset_devices

-- COMMAND ----------

-- When trying to use the MERGE UPSERT method, I was running into an issue where duplicate rows would be
-- added. To mitigate this, I'm opted for deleting and rewriting the last seven days worth of rows to ensure
-- I don't have any duplicates that are being added.
create or replace temp view vg3x_metrics_daily_flash_reset_updates as (
 select * from vg_daily_flash_reset_devices
  where date >= date_sub(current_date(), 10)
);


delete from data_analytics.dataprep_vg3x_flash_reset_by_day
where date >= date_sub(current_date(), 10);
  
merge into data_analytics.dataprep_vg3x_flash_reset_by_day as target 
using vg3x_metrics_daily_flash_reset_updates as updates 
on target.date = updates.date
when matched then update set *
when not matched then insert *
