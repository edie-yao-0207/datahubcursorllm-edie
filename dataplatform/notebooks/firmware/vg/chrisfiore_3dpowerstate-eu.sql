-- Databricks notebook source
--New 3d notebook for timestamp instead of dates for difference calcs referencing KL's notebook - https://samsara.cloud.databricks.com/?o=8972003451708087#notebook/885576096455205/command/885576096455216

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 1) Grab device list

-- COMMAND ----------

--We're only interested in VG devices
CREATE OR REPLACE TEMP VIEW devices AS (
  SELECT
    org_id,
    id AS device_id
  FROM clouddb.devices
  WHERE product_id IN (24,35,53,89,178)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 2) Get device power state history

-- COMMAND ----------

-- select power state
CREATE OR REPLACE TEMP VIEW vg_powerstate_history AS (
  SELECT
    p.object_id as device_id,
    p.org_id,
    p.value.int_value AS power_state,
    p.value.is_databreak,
    p.value.is_end,
    p.value.time
  FROM kinesisstats.osdpowerstate AS p
  JOIN devices AS d ON
    p.org_id = d.org_id
    AND p.object_id = d.device_id
  WHERE p.date >= '2021-01-01'
     AND p.value.is_databreak = false
     AND p.value.is_end = false
)

-- COMMAND ----------

-- In order to build our transitions, we need to construct the start and end time.
-- Grab prior state and prior time for each row.
CREATE OR REPLACE TEMP VIEW device_power_lag AS (
  SELECT
    org_id,
    device_id,
    COALESCE(lag(time) OVER (PARTITION BY org_id, device_id ORDER BY time), 0) AS prev_time,
    COALESCE(lag(power_state) OVER (PARTITION BY org_id, device_id ORDER BY time), 0) AS prev_state,
    time AS cur_time,
    power_state AS cur_state
  FROM vg_powerstate_history
);

-- Look ahead to grab the next state and time.
CREATE OR REPLACE TEMP VIEW device_power_lead AS (
  SELECT
    org_id,
    device_id,
    time AS prev_time,
    power_state AS prev_state,
    COALESCE(lead(time) OVER (PARTITION BY org_id, device_id ORDER BY time), 0) AS cur_time,
    COALESCE(lead(power_state) OVER (PARTITION BY org_id, device_id ORDER BY time), 0) AS cur_state
  FROM vg_powerstate_history
);

--Create table that is a union of the transitions between each state
CREATE OR REPLACE TEMP VIEW power_hist AS (
  (SELECT *
   FROM device_power_lag lag
   WHERE lag.prev_state != lag.cur_state
  )
  UNION
  (SELECT *
   FROM device_power_lead lead
   WHERE lead.prev_state != lead.cur_state
  )
);

-- COMMAND ----------

-- Since we only grabbed the transitions, we don't see
-- the timestamps for consecutive reported objectStat values. To
-- create accurate intervals, we need to grab the next rows cur_time
-- since that timestamp is the end of the consecutive values (i.e. state transition).
CREATE OR REPLACE TEMP VIEW power_states as (
  SELECT
    org_id,
    device_id,
    cur_time AS start_ms,
    cur_state AS power_state,
    lead(cur_time) OVER (PARTITION BY org_id, device_id ORDER BY cur_time) AS end_ms
  FROM power_hist
);

-- Create final view that have both unpowered and powered ranges
CREATE OR REPLACE TEMP VIEW vg_power_ranges AS (
  SELECT
    to_date(from_unixtime(start_ms/1000, "yyyy-MM-dd")) AS date,
    org_id,
    device_id,
    power_state,
    start_ms,
    COALESCE(end_ms, unix_timestamp(date_sub(current_date(), 1)) * 1000) AS end_ms --fill open interval
  FROM power_states
  WHERE start_ms != 0
)

-- COMMAND ----------

-- There is an edge case where devices will transition into battery read only mode for long periods of time before rebooting
-- and then transition into low power mode. We don't want to consider these intervals of time when looking at time avg time from
-- normal to low power. To get around these intervals, we can create a cumulative sum of the power states and then use the cumsum value as a partition.
CREATE OR REPLACE TEMP VIEW vg_power_ranges_cumsum AS(
  SELECT
    date,
    org_id,
    device_id,
    power_state,
    start_ms,
    end_ms,
    SUM(CASE WHEN power_state = 10 THEN 1 ELSE 0 END) OVER (PARTITION BY org_id, device_id ORDER BY start_ms ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS read_only_state_cumsum,
    SUM(CASE WHEN power_state = 2 THEN 1 ELSE 0 END) OVER (PARTITION BY org_id, device_id ORDER BY start_ms ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS off_state_cumsum
  FROM vg_power_ranges
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 3) Calculate Normal to Low Power Time

-- COMMAND ----------

-- Now that we've constrcuted out power intervals, we can join this table with itsef,-- to filter out any power modes that we're note interested in.
-- We'll filter our brief wake periods for the VG.
CREATE OR REPLACE TEMP VIEW vg_power_ranges_self_join AS (
  SELECT
    a.date,
    a.org_id,
    a.device_id,
    a.start_ms,
    a.end_ms,
    a.read_only_state_cumsum,
    a.off_state_cumsum,
    MIN(b.start_ms) AS first_reported_low_power_ms
  FROM vg_power_ranges_cumsum AS a
  LEFT JOIN vg_power_ranges_cumsum AS b ON
    a.org_id = b.org_id
    AND a.device_id = b.device_id
    AND a.read_only_state_cumsum = b.read_only_state_cumsum
    AND a.off_state_cumsum = b.off_state_cumsum
    AND a.end_ms - a.start_ms > 300000  -- filter out brief wakes, these are typically only 3 minutes we'll do 5 minutes just to be sure
    AND b.start_ms >= a.end_ms
  WHERE a.power_state = 1
    AND b.power_state = 3
  GROUP BY
    a.date,
    a.org_id,
    a.device_id,
    a.start_ms,
    a.end_ms,
    a.read_only_state_cumsum,
    a.off_state_cumsum
)

-- COMMAND ----------

-- After filtering out ant power states that occur between normal and low power modes, we need to account for
-- segments of time where the device never reaches low power mode. This will surface itself as two normal power modes
-- with the same first_reported_low_power_ms joined. 

-- To grab the nearest normal power end time to each first_reported_low_power_ms, we can use the MAX() window function.
CREATE OR REPLACE TEMP VIEW vg_power_ranges_nearest_normal_power AS (
  SELECT
    date,
    org_id,
    device_id,
    start_ms,
    end_ms,
    read_only_state_cumsum,
    off_state_cumsum,
    MAX(end_ms) OVER (PARTITION BY org_id, device_id, read_only_state_cumsum,off_state_cumsum, first_reported_low_power_ms) AS nearest_normal_power_time_ms,  -- Get timestamp of the start of next normal power interval
    first_reported_low_power_ms
  FROM vg_power_ranges_self_join
);

-- WHERE clause to only grab normal power modes where end_ms = nearest_normal_power_time_ms
CREATE OR REPLACE TEMP VIEW vg_power_ranges_nearest_normal_power_filtered AS (
SELECT *
FROM vg_power_ranges_nearest_normal_power
WHERE end_ms = nearest_normal_power_time_ms
)

-- COMMAND ----------

--Calculate the time difference between each normal power state and the following low power state
CREATE OR REPLACE TEMP VIEW normal_low_power_diff AS (
  SELECT
    date,
    org_id,
    device_id,
    start_ms,
    end_ms,
    first_reported_low_power_ms,
    (first_reported_low_power_ms - end_ms)/(1000*60*60) as time_sleep_hours
  FROM vg_power_ranges_nearest_normal_power_filtered
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Step 4) Join normal power transitions to device details

-- COMMAND ----------

-- Grab org timeout settings
CREATE OR REPLACE TEMP VIEW org_timeout_setting AS (
  WITH settings AS (
    SELECT 
      id AS org_id,
      settings_proto.vg_custom_low_power_profile.override_timeout_secs AS timeout_setting,
      explode(sequence(to_date('2021-04-01'), current_date())) AS date
    FROM clouddb.organizations
  )
  
  SELECT
    org_id,
    timeout_setting,
    string(date) as date
  FROM settings
)

-- COMMAND ----------

-- Join daily summary table to grab additional device info
CREATE OR REPLACE TEMP VIEW normal_low_power_diff_device_details AS (
  SELECT 
    a.date,
    a.org_id,
    b.org_name,
    b.org_type,
    o.timeout_setting,
    a.device_id,
    b.product_type,
    b.product_version,
    b.rollout_stage_id,
    b.product_program_id,
    b.product_program_id_type,
    b.can_bus_type,
    CASE
      WHEN b.trip_count IS NOT NULL THEN "Active"
      ELSE "Alive"
    END AS status,
    b.latest_build_on_day,
    b.has_modi,
    a.start_ms,
    a.end_ms,
    a.first_reported_low_power_ms,
    a.time_sleep_hours
  FROM normal_low_power_diff AS a
  LEFT JOIN data_analytics.dataprep_vg3x_daily_health_metrics AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.device_id = b.device_id
  LEFT JOIN org_timeout_setting AS o ON
    a.org_id = o.org_id
    AND a.date = o.date
  WHERE a.date >= '2021-01-01'
  AND a.date <= date_sub(current_date(), 1)
  AND a.org_id NOT IN (1,0,18103)
)

-- COMMAND ----------

-- Create final table for Tableau
CREATE TABLE IF NOT EXISTS data_analytics.chrisfiore_3dpowerstate USING DELTA
SELECT * FROM normal_low_power_diff_device_details

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW power_state_updates AS (
  SELECT *
  FROM normal_low_power_diff_device_details
  WHERE date >= date_sub(current_date(),7)
);


MERGE INTO data_analytics.chrisfiore_3dpowerstate AS target
USING power_state_updates AS updates ON
target.date = updates.date
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
AND target.start_ms = updates.start_ms
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### EDA

-- COMMAND ----------

--spot check final data table
-- select
--   date,
--   org_id,
--   org_type,
--   timeout_setting,
--   device_id,
--   product_id,
--   camera_product_id,
--   can_bus_type,
--   latest_build_on_day,
--   has_modi,
--   start_ms,
--   end_ms,
--   from_utc_timestamp(to_timestamp(from_unixtime(start_ms/1000, 'yyyy-MM-dd HH:mm:ss')), 'America/Los_Angeles') as start_time,
--   from_utc_timestamp(to_timestamp(from_unixtime(end_ms/1000, 'yyyy-MM-dd HH:mm:ss')), 'America/Los_Angeles') as end_time,
--   from_utc_timestamp(to_timestamp(from_unixtime(first_reported_low_power_ms/1000, 'yyyy-MM-dd HH:mm:ss')), 'America/Los_Angeles') as first_reported_low_power_ms,
--   time_sleep_hours
-- from data_analytics.chrisfiore_3dpowerstate
-- order by time_sleep_hours DESC
