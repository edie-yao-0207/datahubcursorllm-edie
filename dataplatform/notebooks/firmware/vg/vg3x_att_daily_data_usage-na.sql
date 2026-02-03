-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
-- MAGIC spark.conf.set("spark.sql.broadcastTimeout", 72000)
-- MAGIC spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", 'CORRECTED')

-- COMMAND ----------

--Grab unique device_id, iccid pairs from kinesisstats.osdcellularinterfacestate to mitigtate duplicates. 
-- Only using the iccid from the clouddb.devices will drop a lot of data because it only grabs that last 
-- reported ICCID used. We can igonore null operator_s values since those are recorded when a connection to a given operator ends.
CREATE OR REPLACE TEMP VIEW device_iccid_pairs AS (
  SELECT DISTINCT
    a.date,
    a.org_id,
    a.object_id,
    a.value.proto_value.interface_state.cellular.iccid
  FROM kinesisstats.osdcellularinterfacestate AS a
  JOIN data_analytics.vg3x_daily_summary AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.object_id = b.device_id
  WHERE a.value.is_end = 'false'
    AND a.value.is_databreak = 'false'
    AND a.value.proto_value.interface_state.cellular.operator_s IS NOT NULL
)

-- COMMAND ----------

-- Device level granularity data usage. Right join osdcellularinterfacestate with our Jasper data that way we can 
-- have a more accurate mapping of the SIM data usage. Only using the iccid from the clouddb.devices will drop a lot 
-- of data because it only grabs that last reported ICCID used. We can igonore null operator_s values since those
-- are recorded when a connection to a given operator ends.
CREATE OR REPLACE TEMP VIEW device_data_usage AS (
  SELECT
    a.date,
    a.org_id,
    a.object_id AS device_id,
    a.iccid,
    coalesce(c.network, CAST(b.operator_network AS STRING)) AS operator_network,
    coalesce(sum(b.data_usage)/1000000,0) AS total_att_mb_data_used
  FROM device_iccid_pairs AS a
  RIGHT JOIN dataprep_cellular.att_daily_usage AS b ON 
    a.iccid = b.iccid
    AND a.date = b.record_open_date
  LEFT JOIN data_analytics.att_operator_id_reference AS c ON 
    b.operator_network = c.carrier_id
  WHERE b.record_open_date < '2030-01-01' --filtering out some weird values seen in table
  GROUP BY
    a.date,
    a.org_id,
    a.object_id,
    a.iccid,
    b.operator_network,
    c.network
);

-- COMMAND ----------

-- Left join onto data_analytics.dataprep_vg3x_daily_health_metrics so we can get the total number of distinct devices on a given day
CREATE OR REPLACE TEMP VIEW vg3x_daily_data_usage AS (
  SELECT 
    a.date,
    a.org_id,
    a.org_type,
    a.org_name,
    a.product_type,
    a.can_bus_type,
    a.cable_type,
    a.has_modi,
    a.has_baxter,
    a.latest_build_on_day,
    a.battery_status,
    a.status,
    a.product_version,
    a.rollout_stage_id,
    a.product_program_id,
    a.product_program_id_type,
    b.operator_network,
   sum(coalesce(b.total_att_mb_data_used,0)) AS total_att_mb_data_used,
   count(distinct a.device_id) AS total_devices
  FROM data_analytics.dataprep_vg3x_daily_health_metrics AS a
  LEFT JOIN device_data_usage AS b ON 
   a.date = b.date
   AND a.org_id = b.org_id
   AND a.device_id = b.device_id
  GROUP BY
    a.date,
    a.org_id,
    a.org_type,
    a.org_name,
    a.product_type,
    a.can_bus_type,
    a.cable_type,
    a.has_modi,
    a.has_baxter,
    a.latest_build_on_day,
    a.battery_status,
    a.status,
    a.product_version,
    a.rollout_stage_id,
    a.product_program_id,
    a.product_program_id_type,
    b.operator_network
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.vg3x_att_daily_data_usage
USING DELTA
PARTITIONED BY (date)
SELECT *  FROM vg3x_daily_data_usage

-- COMMAND ----------

-- Update calculated fields for the last seven days using a delete and merge clause.
-- Upsert method was causing dupes
-- When trying to use the MERGE UPSERT method, I was running into an issue where duplicate rows would be
-- added. To mitigate this, I opted for deleting and rewriting the last seven days worth of rows to ensure
-- duplicates aren't being created.
CREATE OR REPLACE TEMP VIEW vg3x_daily_data_usage_updates AS (
 SELECT * FROM vg3x_daily_data_usage
  WHERE date >= date_sub(current_date(), 10)
);

DELETE FROM data_analytics.vg3x_att_daily_data_usage
WHERE date >= date_sub(current_date(), 10);
  
MERGE INTO data_analytics.vg3x_att_daily_data_usage AS target 
USING vg3x_daily_data_usage_updates AS updates ON
target.date = updates.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
