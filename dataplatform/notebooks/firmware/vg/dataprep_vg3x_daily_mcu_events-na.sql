-- Databricks notebook source
-- This objectStat is only recorded by VG3x with Modi or Thor devices 
CREATE OR REPLACE TEMP VIEW daily_device_mcu_fatal_counts AS (
   SELECT DISTINCT 
      a.date,
      a.org_id,
      a.org_type,
      a.org_name,
      a.device_id,
      a.product_type,
      a.rollout_stage_id,
      a.product_program_id,
      a.product_program_id_type,
      a.latest_build_on_day,
      COALESCE(a.can_bus_type, 'null') as can_bus_type,
      a.has_modi,
      a.has_baxter,
      a.status,
      a.product_version,
      COALESCE(b.value.proto_value.vg_mcu_fatal.mcu_fw_version, 'null') AS mcu_fw_version,
      COALESCE(b.value.proto_value.vg_mcu_fatal.reason, 'null') AS reason,
      count(b.value.proto_value.vg_mcu_fatal.reason) AS count
    FROM data_analytics.dataprep_vg3x_daily_health_metrics AS a
    LEFT JOIN kinesisstats.osDVgMcuFatal AS b ON
      a.date = b.date
      AND a.device_id = b.object_id
      AND a.org_id = b.org_id
      AND b.value.is_databreak = 'false' 
      AND b.value.is_end = 'false'
    WHERE (a.has_modi = 'true' OR a.product_type like "%VG5%")
      --AND a.date >= date_sub(current_date(),30)
    GROUP BY
      a.date,
      a.org_id,
      a.org_type,
      a.org_name,
      a.device_id,
      a.product_type,
      a.rollout_stage_id,
      a.product_program_id,
      a.product_program_id_type,
      a.latest_build_on_day,
      COALESCE(a.can_bus_type, 'null'),
      a.has_modi,
      a.has_baxter,
      a.status,
      a.product_version,
      COALESCE(b.value.proto_value.vg_mcu_fatal.mcu_fw_version, 'null'),
      COALESCE(b.value.proto_value.vg_mcu_fatal.reason, 'null')
  );

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.dataprep_vg3x_daily_mcu_events USING DELTA
PARTITIONED BY (date) AS
SELECT * FROM daily_device_mcu_fatal_counts

-- COMMAND ----------

--update calculated fields for the last seven days
CREATE OR REPLACE TEMP VIEW daily_device_mcu_fatal_counts_updates AS (
  SELECT DISTINCT *
  FROM daily_device_mcu_fatal_counts
  WHERE date > date_sub(current_date(),10)
);

MERGE INTO data_analytics.dataprep_vg3x_daily_mcu_events AS target
USING daily_device_mcu_fatal_counts_updates AS updates ON
target.date = updates.date 
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
AND target.mcu_fw_version = updates.mcu_fw_version
AND target.reason = updates.reason
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
