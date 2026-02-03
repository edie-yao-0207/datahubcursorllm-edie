-- Databricks notebook source
--Get wake reasons that should result from an engine ON state
CREATE OR REPLACE TEMP VIEW wake_reasons AS (
  SELECT
    date,
    org_id,
    object_id AS device_id,
    value.proto_value.power_state_info.change_reason AS wake_reason
  FROM kinesisstats.osdpowerstate
  WHERE 
    value.proto_value.power_state_info.change_reason IN (0, 2, 3, 4, 5)
    AND value.is_end = 'false'
    AND value.is_databreak = 'false'
);

--Power State Enum  
-- 0  then "UNKNOWN"
-- 1  then "TIME"
-- 2  then "ENGINE_ON"
-- 3  then "ACCEL_MOVEMENT"
-- 4  then "GPS_MOVEMENT"
-- 5  then "VOLTAGE_JUMP"
-- 6  then "SLEEP_DISABLED"
-- 7  then "VIDEO_RETRIEVAL"
-- 8  then "BOOT"
-- 9  then "LOW_VEHICLE_BATTERY"
-- 10 then "EXTERNAL_POWER_CHANGE"
-- 11 then "OBD_MOVEMENT"
-- 12 then "UPGRADING"
-- 13 then "PANIC_BUTTON"
-- 14 then "SMARTCARD_READER"
-- 15 then "NO_GPS"
-- 16 then "AP_CLIENTS"

-- COMMAND ----------

--Get total number of wakes by wake reason
CREATE OR REPLACE TEMP VIEW vg_daily_wake_count AS (
  SELECT
    a.date,
    a.org_id,
    a.device_id,
    a.wake_reason,
    count(a.wake_reason) AS wake_count
  FROM wake_reasons AS a
  JOIN data_analytics.dataprep_vg3x_daily_health_metrics AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.device_id = b.device_id
  GROUP BY 
    a.date,
    a.org_id,
    a.device_id,
    a.wake_reason
)

-- COMMAND ----------

--Join wake counts onto device level summary table
CREATE OR REPLACE TEMP VIEW device_wake_metrics AS (
  SELECT
    a.date,
    a.org_id,
    a.org_type,
    a.org_name,
    a.device_id,
    a.product_type,
    a.latest_build_on_day,
    a.status,
    a.trip_count,
    COALESCE(a.can_bus_type, 'null') AS can_bus_type,
    a.cable_type,
    a.has_modi,
    a.has_baxter,
    a.product_version,
    a.rollout_stage_id,
    a.product_program_id,
    a.product_program_id_type,
    coalesce(b.wake_reason, 'null') AS wake_reason,
    coalesce(b.wake_count, 0) AS wake_count
  FROM data_analytics.dataprep_vg3x_daily_health_metrics AS a
  LEFT JOIN vg_daily_wake_count AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.device_id = b.device_id
)

-- COMMAND ----------

--Daily level aggregation for tableau
CREATE OR REPLACE TEMP VIEW device_wake_metrics_agg AS (
  SELECT
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    latest_build_on_day,
    status,
    trip_count,
    can_bus_type,
    has_modi,
    has_baxter,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    wake_reason,
    sum(wake_count) AS wake_count,
    sum(trip_count) AS total_trips,
    count(distinct device_id) AS total_devices
  FROM device_wake_metrics
  GROUP BY
    date,
    org_id,
    org_type,
    org_name,
    product_type,
    latest_build_on_day,
    status,
    trip_count,
    can_bus_type,
    has_modi,
    has_baxter,
    product_version,
    rollout_stage_id,
    product_program_id,
    product_program_id_type,
    wake_reason
    
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.vg_wake_reason_distributions USING DELTA
PARTITIONED BY (date)
SELECT * FROM device_wake_metrics_agg

-- COMMAND ----------

-- When trying to use the MERGE UPSERT method, I was running into an issue where duplicate rows would be
-- added. To mitigate this, I'm opted for deleting and rewriting the last seven days worth of rows to ensure
-- I don't have any duplicates that are being added.
CREATE OR REPLACE TEMP VIEW device_wake_metrics_updates AS (
 SELECT * FROM device_wake_metrics_agg
  WHERE date >= date_sub(current_date(), 10)
);

DELETE FROM data_analytics.vg_wake_reason_distributions
WHERE date >= date_sub(current_date(), 10);
  
MERGE INTO data_analytics.vg_wake_reason_distributions AS target 
USING device_wake_metrics_updates AS updates ON
target.date = updates.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Appendix: EDA

-- COMMAND ----------

-- --Visualize daily aggregate data
-- with total_aggs as (
--   select
--     date,
--     sum(total_wakes) as total_wakes
--   from playground.wake_metrics_test
--   group by
--     date
-- ),

-- count_aggs as (
--   select
--   date,
--   wake_reason,
--   sum(wake_count) as wake_count
--   from playground.wake_metrics_test
--   group by 
--     date,
--     wake_reason
-- )

-- select
-- a.date,
-- b.wake_reason,
-- b.wake_count,
-- a.total_wakes,
-- b.wake_count / a.total_wakes as ratio
-- from total_aggs as a
-- join count_aggs as b on
-- a.date = b.date

-- COMMAND ----------

-- --Aggregate at daily level
-- create or replace temp view vg_daily_wake_metrics as (
--   select
--     date,
--     org_id,
--     org_type,
--     org_name,
--     product_type,
--     latest_build_on_day,
--     status,
--     trip_count,
--     can_bus_type,
--     gw_first_heartbeat_date,
--     wake_reason,
--     sum(wake_count) as wake_count,
--     sum(total_wakes) as total_wakes,
--     sum(trip_count) as total_trips
--   from device_wake_metrics
--   group by
--     date,
--     org_id,
--     org_type,
--     org_name,
--     product_type,
--     latest_build_on_day,
--     status,
--     trip_count,
--     can_bus_type,
--     gw_first_heartbeat_date,
--     wake_reason
-- )
