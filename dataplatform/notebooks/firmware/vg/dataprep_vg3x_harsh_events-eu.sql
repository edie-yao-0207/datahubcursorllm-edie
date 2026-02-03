-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW harsh_events_labeled AS (
 SELECT 
   a.date,
   a.org_id,
   a.device_id,
   a.vehicle_type,
   a.imu_orienter_disabled,
   COALESCE(enums.event_type, CAST(a.harsh_accel_type AS string)) AS harsh_event_type,
   a.oriented_harsh_detector_triggered,
   a.passes_speed_filter,
   a.passed_threshold_filter,
   a.passed_speed_filter,
   a.passed_trip_filter,
   a.passed_inbox_filter
 FROM data_analytics.vg3x_harsh_events_w_filters AS a
 LEFT JOIN definitions.harsh_accel_type_enums AS enums ON
      a.harsh_accel_type = enums.enum
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW  daily_device_harsh_event_counts AS (
 SELECT 
   a.date,
   a.org_id,
   a.org_type,
   a.org_name,
   a.device_id,
   a.product_type,
   a.product_version,
   a.latest_build_on_day,
   a.can_bus_type,
   a.has_modi,
   a.cable_type,
   a.status,
   a.total_distance_meters,
   a.gw_first_heartbeat_date,
   a.rollout_stage_id,
   a.product_program_id,
   a.product_program_id_type,
   b.vehicle_type,
   b.imu_orienter_disabled,
   b.oriented_harsh_detector_triggered,
   b.harsh_event_type,
   CASE WHEN b.passed_threshold_filter = 1 THEN true ELSE false END AS passed_threshold_filter,
   CASE WHEN b.passed_speed_filter= 1 THEN true ELSE false END AS passed_speed_filter,
   CASE WHEN b.passed_trip_filter = 1 THEN true ELSE false END AS passed_trip_filter,
   CASE WHEN b.passed_inbox_filter = 1 THEN true ELSE false END AS passed_inbox_filter,
    COUNT(*) as harsh_event_count
  FROM data_analytics.dataprep_vg3x_daily_health_metrics AS a
  JOIN harsh_events_labeled AS b ON
    a.date = b.date
   AND a.org_id = b.org_id
   AND a.device_id = b.device_id
 GROUP BY
    a.date,
    a.org_id,
    a.org_type,
    a.org_name,
    a.device_id,
    a.product_type,
    a.product_version,
    a.latest_build_on_day,
    a.can_bus_type,
    a.has_modi,
    a.cable_type,
    a.status,
    a.total_distance_meters,
    a.gw_first_heartbeat_date,
    a.rollout_stage_id,
    a.product_program_id,
    a.product_program_id_type,
    b.vehicle_type,
    b.imu_orienter_disabled,
    b.oriented_harsh_detector_triggered,
    b.harsh_event_type,
    b.passed_threshold_filter,
    b.passed_speed_filter,
    b.passed_trip_filter,
    b.passed_inbox_filter
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.dataprep_vg3x_harsh_events
USING DELTA
PARTITIONED BY (date)
SELECT * FROM daily_device_harsh_event_counts

-- COMMAND ----------

-- Update calculated fields for the last seven days using a delete and merge clause.
-- Upsert method was causing dupes
CREATE OR REPLACE TEMP VIEW dataprep_vg3x_harsh_events_updates AS (
  SELECT *
  FROM daily_device_harsh_event_counts
  WHERE date >= date_sub(CURRENT_DATE(),10)
);

DELETE FROM data_analytics.dataprep_vg3x_harsh_events
WHERE date >= date_sub(CURRENT_DATE(), 10);
  
MERGE INTO data_analytics.dataprep_vg3x_harsh_events AS target 
USING dataprep_vg3x_harsh_events_updates AS updates 
ON target.date = updates.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
