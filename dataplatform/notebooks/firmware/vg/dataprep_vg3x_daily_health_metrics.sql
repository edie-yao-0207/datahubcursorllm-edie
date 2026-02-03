-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW oom_err_count AS (
  SELECT
    a.date,
    a.org_id,
    a.device_id,
    count(b.value.proto_value.anomaly_event.service_name) AS oom_count
  FROM data_analytics.vg3x_daily_summary AS a
  LEFT JOIN kinesisstats.osdanomalyevent AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.device_id = b.object_id
  WHERE b.value.proto_value.anomaly_event.service_name = "kernel-panic-oom"
    AND b.value.is_end = 'false'
    AND b.value.is_databreak = 'false'
  GROUP BY
    a.date,
    a.org_id,
    a.device_id
)

-- COMMAND ----------

 --Get count of how many VG heartbearts were in compromised range
CREATE OR REPLACE TEMP VIEW compromised_batt_list AS (
  SELECT
    a.date,
    a.org_id,
    a.device_id,
    b.battery_status
  FROM data_analytics.vg3x_daily_summary AS a
  LEFT JOIN data_analytics.dataprep_vg_daily_compromised_batts AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.device_id = b.object_id
 );

-- COMMAND ----------

--Get first heartbeat of each device
CREATE OR REPLACE TEMP VIEW gw_first_last_hbs AS (
  SELECT
    org_id,
    device_id,
    product_id,
    min(date) AS gw_first_heartbeat_date,
    max(date) AS gw_last_heartbeat_date
  FROM data_analytics.vg3x_daily_summary
  GROUP BY
    org_id,
    device_id,
    product_id
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW daily_metrics AS (
  SELECT DISTINCT
    a.date,
    a.org_id,
    a.org_type,
    a.org_name,
    a.device_id,
    CASE
      WHEN a.product_id = 24 AND a.camera_product_id = 30 THEN "VG34 + CM12"
      WHEN a.product_id = 24 AND a.camera_product_id = 31 THEN "VG34 + CM22"
      WHEN a.product_id = 24 AND a.camera_product_id = 43 THEN "VG34 + CM32"
      WHEN a.product_id = 24 AND a.camera_product_id = 44 THEN "VG34 + CM31"
      WHEN a.product_id = 24 AND a.camera_product_id = 155 THEN "VG34 + CM34"
      WHEN a.product_id = 24 AND a.camera_product_id = 167 THEN "VG34 + CM33"
      WHEN a.product_id = 35 AND a.camera_product_id = 30 THEN "VG34-EU + CM12"
      WHEN a.product_id = 35 AND a.camera_product_id = 31 THEN "VG34-EU + CM22"
      WHEN a.product_id = 35 AND a.camera_product_id = 43 THEN "VG34-EU + CM32"
      WHEN a.product_id = 35 AND a.camera_product_id = 44 THEN "VG34-EU + CM31"
      WHEN a.product_id = 35 AND a.camera_product_id = 155 THEN "VG34-EU + CM34"
      WHEN a.product_id = 35 AND a.camera_product_id = 167 THEN "VG34-EU + CM33"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) AND a.camera_product_id = 30 THEN "VG54-NA + CM12"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) AND a.camera_product_id = 31 THEN "VG54-NA + CM22"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) AND a.camera_product_id = 43 THEN "VG54-NA + CM32"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) AND a.camera_product_id = 44 THEN "VG54-NA + CM31"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) AND a.camera_product_id = 155 THEN "VG54-NA + CM34"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) AND a.camera_product_id = 167 THEN "VG54-NA + CM33"
      WHEN a.product_id = 53 AND a.variant_id IN (10) AND a.camera_product_id = 30 THEN "VG54-NAH + CM12"
      WHEN a.product_id = 53 AND a.variant_id IN (10) AND a.camera_product_id = 31 THEN "VG54-NAH + CM22"
      WHEN a.product_id = 53 AND a.variant_id IN (10) AND a.camera_product_id = 43 THEN "VG54-NAH + CM32"
      WHEN a.product_id = 53 AND a.variant_id IN (10) AND a.camera_product_id = 44 THEN "VG54-NAH + CM31"
      WHEN a.product_id = 53 AND a.variant_id IN (10) AND a.camera_product_id = 155 THEN "VG54-NAH + CM34"
      WHEN a.product_id = 53 AND a.variant_id IN (10) AND a.camera_product_id = 167 THEN "VG54-NAH + CM33"
      WHEN a.product_id = 89 AND a.camera_product_id = 30 THEN "VG54-EU + CM12"
      WHEN a.product_id = 89 AND a.camera_product_id = 31 THEN "VG54-EU + CM22"
      WHEN a.product_id = 89 AND a.camera_product_id = 43 THEN "VG54-EU + CM32"
      WHEN a.product_id = 89 AND a.camera_product_id = 44 THEN "VG54-EU + CM31"
      WHEN a.product_id = 89 AND a.camera_product_id = 155 THEN "VG54-EU + CM34"
      WHEN a.product_id = 89 AND a.camera_product_id = 167 THEN "VG54-EU + CM33"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) AND a.camera_product_id = 30 THEN "VG55-NA + CM12"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) AND a.camera_product_id = 31 THEN "VG55-NA + CM22"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) AND a.camera_product_id = 43 THEN "VG55-NA + CM32"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) AND a.camera_product_id = 44 THEN "VG55-NA + CM31"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) AND a.camera_product_id = 155 THEN "VG55-NA + CM34"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) AND a.camera_product_id = 167 THEN "VG55-NA + CM33"
      WHEN a.product_id = 178 AND a.variant_id IN (16) AND a.camera_product_id = 30 THEN "VG55-EU + CM12"
      WHEN a.product_id = 178 AND a.variant_id IN (16) AND a.camera_product_id = 31 THEN "VG55-EU + CM22"
      WHEN a.product_id = 178 AND a.variant_id IN (16) AND a.camera_product_id = 43 THEN "VG55-EU + CM32"
      WHEN a.product_id = 178 AND a.variant_id IN (16) AND a.camera_product_id = 44 THEN "VG55-EU + CM31"
      WHEN a.product_id = 178 AND a.variant_id IN (16) AND a.camera_product_id = 155 THEN "VG55-EU + CM34"
      WHEN a.product_id = 178 AND a.variant_id IN (16) AND a.camera_product_id = 167 THEN "VG55-EU + CM33"
      WHEN a.product_id = 178 AND a.variant_id IN (17) AND a.camera_product_id = 30 THEN "VG55-FN + CM12"
      WHEN a.product_id = 178 AND a.variant_id IN (17) AND a.camera_product_id = 31 THEN "VG55-FN + CM22"
      WHEN a.product_id = 178 AND a.variant_id IN (17) AND a.camera_product_id = 43 THEN "VG55-FN + CM32"
      WHEN a.product_id = 178 AND a.variant_id IN (17) AND a.camera_product_id = 44 THEN "VG55-FN + CM31"
      WHEN a.product_id = 178 AND a.variant_id IN (17) AND a.camera_product_id = 155 THEN "VG55-FN + CM34"
      WHEN a.product_id = 178 AND a.variant_id IN (17) AND a.camera_product_id = 167 THEN "VG55-FN + CM33"
      WHEN a.product_id = 24 THEN "VG34 no CM"
      WHEN a.product_id = 35 THEN "VG34-EU no CM"
      WHEN a.product_id = 53 AND a.variant_id NOT IN (10) THEN "VG54-NA no CM"
      WHEN a.product_id = 53 AND a.variant_id IN (10) THEN "VG54-NAH no CM"
      WHEN a.product_id = 89 THEN "VG54-EU no CM"
      WHEN a.product_id = 178 AND a.variant_id NOT IN (16, 17) THEN "VG55-NA no CM"
      WHEN a.product_id = 178 AND a.variant_id IN (16) THEN "VG55-EU no CM"
      WHEN a.product_id = 178 AND a.variant_id IN (17) THEN "VG55-FN no CM"
      ELSE "error"
    END AS product_type,
    a.latest_build_on_day,
    CASE
      WHEN a.trip_count = 0 THEN 'Alive'
      ELSE 'Active'
    END AS status,
    COALESCE(j.can_bus_name, CAST(a.can_bus_type AS string)) AS can_bus_type,
    COALESCE(k.cable_name, CAST(a.cable_type AS string)) AS cable_type,
    a.has_modi,
    a.has_baxter,
    a.has_octo,
    a.trip_count,
    a.total_distance_meters,
    a.product_version,
    a.rollout_stage_id,
    a.product_program_id,
    a.product_program_id_type,
    COALESCE(b.gps_count,0) AS gps_count,
    b.max_acc_milli/1000 AS max_acc_meter,
    b.mean_acc_milli/1000 AS mean_acc_meter,
    b.min_acc_milli/1000 AS min_acc_meter,
    c.99th_percentile_memory_usage,
    c.median_memory_usage,
    d.gw_first_heartbeat_date,
    d.gw_last_heartbeat_date,
    COALESCE(f.oom_count, 0) AS oom_count,
    COALESCE(g.battery_status, 'Healthy') AS battery_status,
    h.spi_comms_time,
    h.normal_power_time,
    i.median_time_fix_to_server_ms AS median_time_fix_to_server_ms,
    i.95th_perc_time_fix_to_server_ms AS 95th_perc_time_fix_to_server_ms
  FROM data_analytics.vg3x_daily_summary AS a
  LEFT JOIN dataprep.device_gps_accuracy AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.device_id = b.device_id
  LEFT JOIN data_analytics.vg_memory_usage AS c ON
    a.date = c.date
    AND a.org_id = c.org_id
    AND a.device_id = c.device_id
  LEFT JOIN gw_first_last_hbs AS d ON
    a.org_id = d.org_id
    AND a.device_id = d.device_id
    AND a.product_id = d.product_id
  LEFT JOIN oom_err_count AS f ON
    a.date = f.date
    AND a.org_id = f.org_id
    AND a.device_id = f.device_id
  LEFT JOIN compromised_batt_list AS g ON
    a.date = g.date
    AND a.org_id = g.org_id
    AND a.device_id = g.device_id
  LEFT JOIN data_analytics.spi_uptime AS h ON
    a.date = h.date
    AND a.org_id = h.org_id
    AND a.device_id = h.device_id
  LEFT JOIN data_analytics.vg3x_gps_fix_times AS i ON
    a.date = i.date
    AND a.org_id = i.org_id
    AND a.device_id = i.device_id
  LEFT JOIN data_analytics.canbus_names AS j ON
    a.can_bus_type = j.can_bus_id
  LEFT JOIN data_analytics.cable_names AS k ON
    a.cable_type = k.cable_id
    AND a.product_id = k.product_id
  WHERE a.org_id NOT IN (0,1,18103)
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.dataprep_vg3x_daily_health_metrics
USING DELTA
PARTITIONED BY (date)
SELECT * FROM daily_metrics

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vg3x_daily_metrics_updates AS (
  SELECT DISTINCT * FROM daily_metrics
  WHERE date >= date_sub(current_date(),10) OR
  gw_last_heartbeat_date >= date_sub(current_date(), 10)
);

MERGE INTO data_analytics.dataprep_vg3x_daily_health_metrics AS target
USING vg3x_daily_metrics_updates AS updates ON
target.date = updates.date
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
