-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW vg3x_metrics_daily_aggreation AS (
  SELECT DISTINCT 
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    COALESCE(can_bus_type, 'null') AS can_bus_type,
    cable_type,
    has_modi,
    has_baxter,
    has_octo,
    latest_build_on_day,
    battery_status,
    status,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    sum(case when trip_count is not null then 1 else 0 end) AS active_device_count,
    sum(trip_count) AS trip_count,
    sum(total_distance_meters) AS total_distance_meters,
    sum(gps_count) AS gps_count,
    sum(oom_count) AS oom_count,
    sum(spi_comms_time) AS spi_comms_time,
    sum(normal_power_time) AS normal_power_time,
    max(max_acc_meter) AS max_acc_meter,
    mean(mean_acc_meter) AS mean_acc_meter,
    min(min_acc_meter) AS min_acc_meter,
    percentile_approx(99th_percentile_memory_usage,.5) AS median_99th_memory_usage,
    percentile_approx(median_memory_usage,.5) AS median_median_memory_usage,
    percentile_approx(median_time_fix_to_server_ms,.5) AS median_median_time_fix_to_server_ms,
    percentile_approx(95th_perc_time_fix_to_server_ms,.5) AS median_95th_perc_time_fix_to_server_ms,
    count(distinct device_id) as device_count
  FROM data_analytics.dataprep_vg3x_daily_health_metrics
  GROUP BY
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    can_bus_type,
    cable_type,
    has_modi,
    has_baxter,
    has_octo,
    latest_build_on_day,
    battery_status,
    status,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.dataprep_vg3x_health_metrics_by_day USING DELTA
PARTITIONED BY (date)
SELECT * FROM  vg3x_metrics_daily_aggreation

-- COMMAND ----------

-- create or replace table data_analytics.dataprep_vg3x_health_metrics_by_day
-- using delta
-- PARTITIONED BY (date)
-- select * from vg3x_metrics_daily_aggreation

-- COMMAND ----------

-- When trying to use the MERGE UPSERT method, I was running into an issue where duplicate rows would be
-- added. To mitigate this, I opted for deleting and rewriting the last seven days worth of rows to ensure
-- duplicates aren't being created.
create or replace temp view vg3x_metrics_daily_aggreation_updates as (
 select DISTINCT * from vg3x_metrics_daily_aggreation
  where date >= date_sub(current_date(), 10)
);

delete from data_analytics.dataprep_vg3x_health_metrics_by_day
where date >= date_sub(current_date(), 10);
  
merge into data_analytics.dataprep_vg3x_health_metrics_by_day as target 
using vg3x_metrics_daily_aggreation_updates as updates 
on target.date = updates.date
when matched then update set *
when not matched then insert *

-- COMMAND ----------


