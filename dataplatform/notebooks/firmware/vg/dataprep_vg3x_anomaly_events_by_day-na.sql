-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW vg3x_daily_anomaly_counts AS (
  SELECT
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    COALESCE(can_bus_type, 'null') AS can_bus_type,
    has_modi,
    has_baxter,
    latest_build_on_day,
    status,
    product_version,
    product_program_id,
    product_program_id_type,
    rollout_stage_id,
    COALESCE(anomaly_event_service, 'null') AS anomaly_event_service,
    sum(ae_count) AS ae_count,
    count(distinct device_id) AS device_count
  FROM data_analytics.dataprep_vg3x_daily_anomalies
  GROUP BY
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    can_bus_type,
    has_modi,
    has_baxter,
    latest_build_on_day,
    status,
    product_version,
    anomaly_event_service,
    product_version,
    product_program_id,
    product_program_id_type,
    rollout_stage_id
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.dataprep_vg3x_anomaly_events_by_day AS (
  SELECT * FROM vg3x_daily_anomaly_counts
)

-- COMMAND ----------

-- When trying to use the MERGE UPSERT method, I was running into an issue where duplicate rows would be
-- added. To mitigate this, I'm opted for deleting and rewriting the last seven days worth of rows to ensure
-- I don't have any duplicates that are being added.
CREATE OR REPLACE TEMP VIEW vg3x_anomaly_counts_updates AS (
 SELECT * FROM vg3x_daily_anomaly_counts
  WHERE date >= date_sub(current_date(), 10)
);

DELETE FROM data_analytics.dataprep_vg3x_anomaly_events_by_day
WHERE date >= date_sub(current_date(), 10);
  
MERGE INTO data_analytics.dataprep_vg3x_anomaly_events_by_day AS target 
USING vg3x_anomaly_counts_updates as updates ON 
target.date = updates.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
