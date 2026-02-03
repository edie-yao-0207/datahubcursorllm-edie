-- Databricks notebook source
-- Grab count of voltages that are between the 4-7V threshold. Although anything < 10.5V is technically
-- a compromised battery, when looking in that range, we were uncovering a lot of devices that ended up
-- charging back up to a healthy voltage. Devices that were truly dead fell well below the 10.5V threshold
-- within a matter of hours. This will ensure that we're not overreporting or mischaracterising compromised
-- batteries.
CREATE OR REPLACE TEMP VIEW device_batteries AS (
  SELECT
    a.date,
    a.org_id,
    a.object_id,
    count(*) AS voltage_count
  FROM kinesisstats.osdcablevoltage AS a
  INNER JOIN data_analytics.vg3x_daily_summary AS b ON
    a.date = b.date
    AND a.org_id = b.org_id
    AND a.object_id = b.device_id
  WHERE
    a.value.int_value >= 4000 AND a.value.int_value <= 7000
    AND a.value.is_databreak = 'false'
    AND a.value.is_end = 'false'
    AND a.date >= date_sub(current_date(),10)
  GROUP BY
    a.date,
    a.org_id,
    a.object_id
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_battery_status AS (
  SELECT
    date,
    org_id,
    object_id,
    CASE
      WHEN voltage_count >= 10 THEN 'Compromised'
      ELSE 'Healthy'
    END AS battery_status
  FROM device_batteries
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.dataprep_vg_daily_compromised_batts
USING DELTA
PARTITIONED BY (date)
SELECT * FROM device_battery_status

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_battery_status_updates AS (
  SELECT * FROM device_battery_status
  WHERE date > date_sub(current_date(),10)
);

MERGE INTO data_analytics.dataprep_vg_daily_compromised_batts AS target
USING device_battery_status_updates AS updates ON
target.date = updates.date
AND target.org_id = updates.org_id
AND target.object_id = updates.object_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
