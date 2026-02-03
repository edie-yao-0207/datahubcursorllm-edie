-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW vg3x_metrics_daily_aggreation AS (
  select
    distinct
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    COALESCE(can_bus_type, 'null') as can_bus_type,
    cable_type,
    has_modi,
    latest_build_on_day,
    status,
    has_tacho,
    percentile_approx(95th_percentile_cpu_usage,.5) AS median_95th_percentile_cpu_usage,
    percentile_approx(median_cpu_usage,.5) AS median_median_cpu_usage,
    percentile_approx(90th_percentile_trip_gap,.5) AS median_90th_percentile_trip_gap,
    percentile_approx(75th_percentile_trip_gap,.5) AS median_75th_percentile_trip_gap,
    percentile_approx(median_trip_gap,.5) AS median_median_percentile_trip_gap,
    SUM(total_trips) AS total_trips,
    SUM(CASE WHEN diagnostics_failed = false THEN 1 ELSE 0 END) AS diagnostics_healthy_device_count,
    SUM(CASE WHEN download_failed = true THEN 1 ELSE 0 END) AS download_failed_device_count,
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
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    can_bus_type,
    cable_type,
    has_modi,
    latest_build_on_day,
    status,
    has_tacho
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.dataprep_vg3x_health_metrics_by_day_extended USING delta
PARTITIONED BY (date)
SELECT * FROM vg3x_metrics_daily_aggreation

-- COMMAND ----------

-- When trying to use the MERGE UPSERT method, I was running into an issue where duplicate rows would be
-- added. To mitigate this, I opted for deleting and rewriting the last seven days worth of rows to ensure
-- duplicates aren't being created.
CREATE OR REPLACE TEMP VIEW vg3x_metrics_daily_aggreation_updates AS (
 SELECT * FROM vg3x_metrics_daily_aggreation
  WHERE date >= date_sub(CURRENT_DATE(), 7)
);

DELETE FROM data_analytics.dataprep_vg3x_health_metrics_by_day_extended
WHERE date >= date_sub(CURRENT_DATE(), 7);
  
MERGE INTO data_analytics.dataprep_vg3x_health_metrics_by_day_extended AS target 
USING vg3x_metrics_daily_aggreation_updates AS updates ON
target.date = updates.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------


