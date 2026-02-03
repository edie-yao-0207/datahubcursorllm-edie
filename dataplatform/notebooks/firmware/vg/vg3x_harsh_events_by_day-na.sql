-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW vg3x_daily_harsh_event_counts AS (
  SELECT
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    COALESCE(can_bus_type, 'null') AS can_bus_type,
    cable_type,
    has_modi,
    has_baxter,
    latest_build_on_day,
    status,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    vehicle_type,
    imu_orienter_disabled,
    oriented_harsh_detector_triggered,
    harsh_event_type,
    passed_threshold_filter,
    passed_speed_filter,
    passed_inbox_filter,
    passed_trip_filter,
    sum(total_distance_meters) AS total_distance_meters,
    sum(harsh_event_count) AS harsh_event_count,
    count(distinct device_id) as device_count
  FROM data_analytics.dataprep_vg3x_harsh_events
  --WHERE date >= date_sub(current_date(),10)
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
    latest_build_on_day,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    status,
    vehicle_type,
    imu_orienter_disabled,
    oriented_harsh_detector_triggered,
    harsh_event_type,
    passed_threshold_filter,
    passed_speed_filter,
    passed_inbox_filter,
    passed_trip_filter
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.vg3x_harsh_events_by_day AS (
  SELECT * FROM vg3x_daily_harsh_event_counts
)

-- COMMAND ----------

-- When trying to use the MERGE UPSERT method, I was running into an issue where duplicate rows would be
-- added. To mitigate this, I'm opted for deleting and rewriting the last seven days worth of rows to ensure
-- I don't have any duplicates that are being added.
CREATE TEMP VIEW vg3x_harsh_event_counts_updates AS (
 SELECT * FROM vg3x_daily_harsh_event_counts
  WHERE date >= date_sub(current_date(), 10)
);

DELETE FROM data_analytics.vg3x_harsh_events_by_day
WHERE date >= date_sub(current_date(), 10);
  
MERGE INTO data_analytics.vg3x_harsh_events_by_day AS target 
USING vg3x_harsh_event_counts_updates AS updates ON
target.date = updates.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
