-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW devices AS (
  SELECT
    date,
    org_id,
    device_id
  FROM data_analytics.vg3x_daily_summary
  WHERE date >= date_sub(current_date(),10)
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_trips AS (
  SELECT
    t.date,
    t.org_id,
    t.device_id,
    t.proto.start.time AS start_time,
    t.proto.end.time AS end_time
  FROM trips2db_shards.trips AS t
  INNER JOIN devices AS d ON
    t.date = d.date
    AND t.org_id = d.org_id
    AND t.device_id = d.device_id
    AND t.date >= date_sub(current_date(),10)
  WHERE t.proto.trip_distance.distance_meters > 10000 --only grab trips greater than 10km
    AND t.version = 101
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW trip_diag_reset_count AS (
  SELECT
    a.date,
    a.org_id,
    a.device_id,
    COALESCE(COUNT(c.*), 0) AS diag_reset_count
  FROM device_trips AS a
  INNER JOIN kinesisstats.osdcanconnected AS c ON
    a.date = c.date
    AND a.org_id = c.org_id
    AND a.device_id = c.object_id
    AND c.time >= a.start_time
    AND c.time <= a.end_time
  WHERE c.value.int_value = 2
  GROUP BY
    a.date,
    a.org_id,
    a.device_id
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.vg_trip_diag_resets USING DELTA
PARTITIONED BY (date)
SELECT * FROM trip_diag_reset_count

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW trip_diag_reset_count_updates AS (
SELECT *
FROM trip_diag_reset_count
WHERE date >= date_sub(current_date(),10)
);

MERGE INTO data_analytics.vg_trip_diag_resets AS target
USING trip_diag_reset_count_updates AS updates ON
target.date = updates.date
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
