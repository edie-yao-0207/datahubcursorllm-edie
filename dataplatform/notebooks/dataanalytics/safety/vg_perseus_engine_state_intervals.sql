-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Step 1) Grab device list

-- COMMAND ----------

--We're only interested in VG devices
CREATE OR REPLACE TEMP VIEW devices AS (
  SELECT
    org_id,
    id AS device_id
  FROM productsdb.devices
  WHERE product_id IN (24,35,53,89)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 2) Get device Engine state history

-- COMMAND ----------

-- select engine state
-- 0 = off
-- 1 = on
-- 2 = idle
CREATE OR REPLACE TEMP VIEW vg_enginestate_history AS (
  SELECT
    p.date,
    p.object_id as device_id,
    p.org_id,
     CASE
       WHEN p.value.int_value = 1 OR p.value.int_value = 2 THEN 1
       ELSE 0
     END AS engine_state,
    p.value.is_databreak,
    p.value.is_end,
    p.value.time
  FROM kinesisstats.osdenginestate AS p
  JOIN devices AS d ON
    p.org_id = d.org_id
    AND p.object_id = d.device_id
  WHERE p.date >= DATE_SUB(CURRENT_DATE(),7)
     AND p.value.is_databreak = false
     --AND p.value.is_end = false
)

-- COMMAND ----------

-- In order to build our transitions, we need to construct the start and end time.
-- Grab prior state and prior time for each row.
CREATE OR REPLACE TEMP VIEW device_enginestate_lag AS (
  SELECT
    org_id,
    device_id,
    COALESCE(lag(time) OVER (PARTITION BY org_id, device_id ORDER BY time), 0) AS prev_time,
    COALESCE(lag(engine_state) OVER (PARTITION BY org_id, device_id ORDER BY time), 0) AS prev_state,
    time AS cur_time,
    engine_state AS cur_state
  FROM vg_enginestate_history
);

-- Look ahead to grab the next state and time.
CREATE OR REPLACE TEMP VIEW device_enginestate_lead AS (
  SELECT
    org_id,
    device_id,
    time AS prev_time,
    engine_state AS prev_state,
    COALESCE(lead(time) OVER (PARTITION BY org_id, device_id ORDER BY time), 0) AS cur_time,
    COALESCE(lead(engine_state) OVER (PARTITION BY org_id, device_id ORDER BY time), 0) AS cur_state
  FROM vg_enginestate_history
);

--Create table that is a union of the transitions between each state
CREATE OR REPLACE TEMP VIEW enginestate_hist AS (
  (SELECT *
   FROM device_enginestate_lag lag
   WHERE lag.prev_state != lag.cur_state
  )
  UNION
  (SELECT *
   FROM device_enginestate_lead lead
   WHERE lead.prev_state != lead.cur_state
  )
);

-- COMMAND ----------


-- Since we only grabbed the transitions, we don't see
-- the timestamps for consecutive reported objectStat values. To
-- create accurate intervals, we need to grab the next rows cur_time
-- since that timestamp is the end of the consecutive values (i.e. state transition).
CREATE OR REPLACE TEMP VIEW engine_states as (
  SELECT
    org_id,
    device_id,
    cur_time AS start_ms,
    cur_state AS engine_state,
    lead(cur_time) OVER (PARTITION BY org_id, device_id ORDER BY cur_time) AS end_ms
  FROM enginestate_hist
);

-- Create final view that have both unpowered and powered ranges
CREATE OR REPLACE TEMP VIEW vg_engine_states AS (
  SELECT
    to_date(from_unixtime(start_ms/1000, "yyyy-MM-dd")) AS date,
    org_id,
    device_id AS vg_device_id,
    engine_state,
    start_ms AS engine_state_start_ms,
    COALESCE(end_ms, unix_timestamp(date_sub(CURRENT_DATE(), 1)) * 1000) AS engine_state_end_ms --fill open interval
  FROM engine_states
  WHERE start_ms != 0
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.vg_perseus_engine_states
USING delta
AS
SELECT * FROM vg_engine_states

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vg_engine_states_updates AS (
  SELECT *
  FROM vg_engine_states
  WHERE date >= DATE_SUB(CURRENT_DATE(),7)
);

MERGE INTO data_analytics.vg_perseus_engine_states AS target
USING vg_engine_states_updates AS updates
ON target.date = updates.date
AND target.org_id = updates.org_id
AND target.vg_device_id = updates.vg_device_id
AND target.engine_state_start_ms = updates.engine_state_start_ms
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
