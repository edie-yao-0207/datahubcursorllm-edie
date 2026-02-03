-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW vg3x_metrics_daily_aggreation AS (
  SELECT DISTINCT
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    COALESCE(can_bus_type, 'null') as can_bus_type,
    cable_type,
    has_modi,
    has_baxter,
    has_octo,
    latest_build_on_day,
    status,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    percentile_approx(95th_percentile_cpu_usage,.5) as median_95th_percentile_cpu_usage,
    percentile_approx(median_cpu_usage,.5) as median_median_cpu_usage,
    percentile_approx(90th_percentile_trip_gap,.5) as median_90th_percentile_trip_gap,
    percentile_approx(75th_percentile_trip_gap,.5) as median_75th_percentile_trip_gap,
    percentile_approx(median_trip_gap,.5) as median_median_percentile_trip_gap,
    sum(total_trips) as total_trips,
    SUM(CASE WHEN perc_late_pings > 0.01 THEN 1 ELSE 0 END) AS total_late_ping_devices,
    SUM(CASE WHEN diag_reset_count > 1 THEN 1 ELSE 0 END) AS total_diag_reset_devices,
    percentile_approx(perc_gps_uptime,.5) as median_perc_gps_uptime,
    percentile_approx(perc_gps_uptime,.95) as 90p_perc_gps_uptime,
    percentile_approx(perc_gps_uptime,.995) as 995p_perc_gps_uptime,
    count(distinct device_id) as device_count
  FROM data_analytics.dataprep_vg3x_daily_health_metrics_extended
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
    status,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.dataprep_vg3x_health_metrics_by_day_extended USING DELTA
PARTITIONED BY (date)
SELECT * FROM vg3x_metrics_daily_aggreation

-- COMMAND ----------

-- create or replace table data_analytics.dataprep_vg3x_health_metrics_by_day_extended
-- using delta
-- PARTITIONED BY (date)
-- select * from vg3x_metrics_daily_aggreation

-- COMMAND ----------

-- When trying to use the MERGE UPSERT method, I was running into an issue where duplicate rows would be
-- added. To mitigate this, I opted for deleting and rewriting the last seven days worth of rows to ensure
-- duplicates aren't being created.
-- RR 01/13/23: Removing the delete, since merging on the entire grain of the table should avoid dupes  --- update, I think this still had dupes

drop table if exists vg3x_metrics_daily_aggreation_updates_cached;

CREATE OR REPLACE TEMP VIEW vg3x_metrics_daily_aggreation_updates AS (
  SELECT DISTINCT * FROM vg3x_metrics_daily_aggreation
  WHERE date >= date_sub(current_date(), 10)
);

CACHE TABLE vg3x_metrics_daily_aggreation_updates_cached SELECT * FROM vg3x_metrics_daily_aggreation_updates;

DELETE FROM data_analytics.dataprep_vg3x_health_metrics_by_day_extended
WHERE date >= (select min(date) from vg3x_metrics_daily_aggreation_updates_cached);


-- MERGE INTO data_analytics.dataprep_vg3x_health_metrics_by_day_extended AS target 
-- USING vg3x_metrics_daily_aggreation_updates AS updates 
-- on target.date = updates.date
-- and target.org_id = updates.org_id
-- and target.org_type = updates.org_type
-- and target.org_name = updates.org_name
-- and target.product_type = updates.product_type
-- and target.can_bus_type = updates.can_bus_type
-- and target.cable_type = updates.cable_type
-- and target.has_modi = updates.has_modi
-- and target.has_baxter = updates.has_baxter
-- and target.has_octo = updates.has_octo
-- and target.latest_build_on_day = updates.latest_build_on_day
-- and target.status = updates.status
-- and target.product_version = updates.product_version
-- and target.rollout_stage_id = updates.rollout_stage_id
-- and target.product_program_id = updates.product_program_id
-- and target.product_program_id_type = updates.product_program_id_type
-- WHEN MATCHED THEN UPDATE SET *
-- WHEN NOT MATCHED THEN INSERT *

insert into data_analytics.dataprep_vg3x_health_metrics_by_day_extended select * from vg3x_metrics_daily_aggreation_updates_cached



-- COMMAND ----------

select date, count(*) from data_analytics.dataprep_vg3x_health_metrics_by_day_extended group by 1

-- COMMAND ----------

select date, latest_build_on_day, avg(median_median_cpu_usage) from data_analytics.dataprep_vg3x_health_metrics_by_day_extended where date = '2023-01-15'  group by 1,2
