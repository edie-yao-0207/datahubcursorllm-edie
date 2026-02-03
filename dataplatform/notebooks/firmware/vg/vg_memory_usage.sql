-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Step 1: Device List

-- COMMAND ----------

-- Only looking back to 4/15/21 since the fix to the total_cpu_util col in kinesisstats.osDSystemStats was rolled out in VG 16
CREATE OR REPLACE TEMP VIEW devices AS (
  SELECT
    date,
    org_id,
    device_id
  FROM data_analytics.vg3x_daily_summary
  WHERE date >= date_sub(current_date(),10)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 2: Define ObjectStat

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW memory_usage AS (
  SELECT
    date,
    org_id,
    object_id,
    time,
    value.proto_value.system_stats.memory_util
  FROM kinesisstats.osDSystemStats
  WHERE value.is_end = 'false'
    AND value.is_databreak = 'false'
    AND date >= date_sub(current_date(),10)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 3: Define Trip Intervals

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_trips AS (
  SELECT
    t.date,
    t.device_id,
    t.org_id,
    t.proto.start.time AS trip_start_ms,
    t.proto.end.time AS trip_end_ms
  FROM trips2db_shards.trips AS t
  JOIN devices AS d ON
    t.date = d.date
    AND t.org_id = d.org_id
    AND t.device_id = d.device_id
 WHERE t.date >= date_sub(current_date(),10)
    AND t.version = 101
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Step 4: Calculate Trip CPU Usage Percentiles

-- COMMAND ----------

-- The total_cpu_util objectStat reports a 10 minute CPU usage by default. There is an edge case where the CPU usage reported will capture CPU usage data just outside of the trip duration (e.g. CPU object stat gets reported within the first minute of a trip.)
-- To filter these out we'll only consider CPU usage values that are reported >=10min after the trip starts. We also want to filter out cases where the trips were less than 10 minutes in length.
CREATE OR REPLACE TEMP VIEW vg_trip_memory_percentiles AS (
  SELECT
    a.date,
    a.org_id,
    a.device_id,
    percentile_approx(b.memory_util,0.95) as 99th_percentile_memory_usage,
    percentile_approx(b.memory_util,0.50) as median_memory_usage
  FROM device_trips AS a
  LEFT JOIN memory_usage AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.device_id = b.object_id
    AND b.time >= a.trip_start_ms + 600000
    AND b.time <= a.trip_end_ms
  WHERE
    a.trip_end_ms - a.trip_start_ms > 600000
  GROUP BY
    a.date,
    a.org_id,
    a.device_id
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.vg_memory_usage USING delta
PARTITIONED BY (date)
SELECT * FROM vg_trip_memory_percentiles

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vg_memory_usage_updates AS
  SELECT * 
  FROM vg_trip_memory_percentiles 
  WHERE date >= date_sub(current_date(),10);
  
MERGE INTO data_analytics.vg_memory_usage AS target
USING vg_memory_usage_updates AS updates ON
target.date = updates.date
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
