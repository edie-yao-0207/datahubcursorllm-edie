-- Databricks notebook source
-- MAGIC %md
-- MAGIC %md
-- MAGIC ##### READ ME
-- MAGIC Date Created: 06/29/21
-- MAGIC
-- MAGIC
-- MAGIC Description:
-- MAGIC - This notebooks is calculating the total gps fix and no_fix time for each VG's stationary and on-trip intervals.
-- MAGIC
-- MAGIC Considerations:
-- MAGIC
-- MAGIC Adding logic to consider "no_fix" intervals that are 10 seconds or less as "has_fix". Jim had vg stationary at his desk and the TDOP would vary over time. The samsara algorithm treats actual fixes that are above the TDOP as not-a-fix. We have a lower (stricter) TDOP threshold while stationary, so it's likely we will occasionally have stationary fixes go above the TDOP threshold and be discarded. This is one example of how the Samsara algorithm ignores short loss of fix. The samsara algorithm reports no fix after a loss of fix for 60 seconds but the backend actually ignores "loss of fix" reports. This brief loss of fix wouldn't affect our high-level GPS tracking. Essentially the gateway ignores losses of fix since we're only reporting stationary locations (e.g. A stationary VG logs location point A before experiencing a brief disconnection to GPS. After establishing connection again it will log location point B. If location point B is within a certain ditance threshold from A then it will send up location point A again.)
-- MAGIC
-- MAGIC Additional Thoughts:
-- MAGIC - While stationary, losing GPS for 10 seconds is probably harmless.
-- MAGIC - But while moving losing GPS for 10 seconds is probably something we care about
-- MAGIC - This is an arbitary threshold that we're setting
-- MAGIC - Stationary TDOP thesh is 2.0
-- MAGIC - Moving TDOP thresh is 5.0
-- MAGIC
-- MAGIC
-- MAGIC Referenced Tables:
-- MAGIC - playground.widget_daily_summary
-- MAGIC - data_analytics.vg3x_daily_summary
-- MAGIC - trips2db_shards.trips
-- MAGIC - kinesisstats.osdGpsHealth
-- MAGIC
-- MAGIC Original Author: Christopher Fiore
-- MAGIC
-- MAGIC Engineering Reviewer: Jim Rowson
-- MAGIC
-- MAGIC Questions? Reach out to [#ask-hw-fw-analytics](https://samsara-rd.slack.com/archives/C024E8L57MM)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW osdgpshealth AS (
  SELECT
    date,
    org_id,
    object_id AS device_id,
    time,
    explode(value.proto_value.gps_health.periods) AS gps_periods
  FROM kinesisstats.osDGpsHealth
  WHERE 
    value.is_end = false
    AND value.is_databreak = false
    AND date >= DATE_SUB(CURRENT_DATE(),10)
)

-- COMMAND ----------

--Adding in logic to label no_fix periods < 10 seconds as has_fix. Labeling periods of time with a gps fice
CREATE OR REPLACE TEMP VIEW osdgpshealth_periods AS (
  SELECT
    date,
    org_id,
    device_id,
    time,
    CASE
      WHEN gps_periods.gps_state = 1 THEN 1
      WHEN gps_periods.gps_state = 2 AND gps_periods.duration_ms <= 10000 THEN 1
      WHEN gps_periods.gps_state = 2 THEN 0
    END AS has_fix,
    gps_periods.duration_ms
  FROM osdgpshealth
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW daily_gps_fix_durations AS (
  SELECT
    date,
    org_id,
    device_id,
    has_fix,
    SUM(duration_ms) AS total_duration_ms
  FROM osdgpshealth_periods
  GROUP BY
    date,
    org_id,
    device_id,
    has_fix
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW daily_gps_uptime AS (
  SELECT
    date,
    org_id,
    device_id,
    (SUM(CASE WHEN has_fix = 1 THEN total_duration_ms ELSE 0 END) * 100) / SUM(total_duration_ms) AS perc_gps_uptime
  FROM daily_gps_fix_durations
  GROUP BY
    date,
    org_id,
    device_id
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.health_metrics_vg_gps_uptime
USING DELTA
PARTITIONED BY (date)
SELECT * FROM daily_gps_uptime

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW health_metrics_vg_gps_uptime_updates AS (
  SELECT *
  FROM daily_gps_uptime
  WHERE date >= date_sub(current_date(),10)
);


MERGE INTO data_analytics.health_metrics_vg_gps_uptime AS target
USING health_metrics_vg_gps_uptime_updates AS updates ON
target.date = updates.date
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
