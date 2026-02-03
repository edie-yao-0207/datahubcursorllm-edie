-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW daily_device_anomaly_counts AS (
 SELECT 
    a.date,
    a.org_id,
    a.org_type,
    a.org_name,
    a.device_id,
    a.product_type,
    a.product_version,
    a.rollout_stage_id,
    a.product_program_id,
    a.product_program_id_type,
    a.latest_build_on_day,
    a.can_bus_type,
    a.has_modi,
    a.status,
    a.gw_first_heartbeat_date,
    COALESCE(b.anomaly_event_service, 'Null') AS anomaly_event_service,
    COALESCE(b.ae_count, 0) AS ae_count
 FROM data_analytics.dataprep_vg3x_daily_health_metrics AS a
 LEFT JOIN dataprep.device_anomalies AS b ON
   a.date = b.date
   AND a.device_id = b.device_id
   AND a.org_id = b.org_id
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.dataprep_vg3x_daily_anomalies
USING DELTA
PARTITIONED BY (date)
SELECT * FROM daily_device_anomaly_counts

-- COMMAND ----------

--update calculated fields for the last seven days
CREATE OR REPLACE TEMP VIEW  dataprep_vg3x_daily_anomalies_updates AS (
  SELECT *
  FROM daily_device_anomaly_counts
  WHERE date > date_sub(CURRENT_DATE(),10)
);

MERGE INTO data_analytics.dataprep_vg3x_daily_anomalies AS target
USING dataprep_vg3x_daily_anomalies_updates AS updates
ON target.date = updates.date 
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
AND target.anomaly_event_service = updates.anomaly_event_service
WHEN MATCHED THEN update SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------


