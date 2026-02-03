-- Databricks notebook source
SET spark.sql.crossJoin.enabled=true

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW devices_need_update AS (
SELECT
  id,
  group_id,
  org_id,
  manual_odometer_meters,
  manual_odometer_updated_at
FROM productsdb.devices
WHERE manual_odometer_meters > 0
  AND date_format(manual_odometer_updated_at, 'yyyy-MM-dd') >= date_sub(current_date(),7)
  AND date_format(manual_odometer_updated_at, 'yyyy-MM-dd') <= current_date()
)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW gps_distances AS (
SELECT
  date,
  org_id,
  object_id,
  time,
  value.double_value AS gps_distance
FROM kinesisstats.osdderivedgpsdistance
WHERE date >= date_sub(current_date(),30)
  AND date <= current_date()
)

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW new_rows AS (
SELECT
  d.id,
  d.group_id,
  d.org_id,
  d.manual_odometer_meters,
  d.manual_odometer_updated_at,
  COALESCE(CAST(MAX(gps_distance) AS BIGINT), 0) AS gps_distance_at_manual_update
FROM devices_need_update AS d
LEFT JOIN gps_distances AS gps
  ON d.id = gps.object_id
  AND d.org_id = gps.org_id
  AND gps.time <= CAST(to_unix_timestamp(d.manual_odometer_updated_at) AS BIGINT) * 1000
GROUP BY
  d.id,
  d.group_id,
  d.org_id,
  d.manual_odometer_meters,
  d.manual_odometer_updated_at
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep.manual_odometer USING DELTA AS (
  SELECT * FROM new_rows
)

-- COMMAND ----------

MERGE INTO dataprep.manual_odometer AS target
USING new_rows AS updates
ON target.id = updates.id
  AND target.group_id = updates.group_id
  AND target.org_id = updates.org_id
-- Update row if we found a more recent manual odometer entry and the associated gps distance is >= the existing one
WHEN MATCHED AND updates.manual_odometer_updated_at > target.manual_odometer_updated_at AND updates.gps_distance_at_manual_update >= target.gps_distance_at_manual_update THEN UPDATE SET
  manual_odometer_meters = updates.manual_odometer_meters,
  manual_odometer_updated_at = updates.manual_odometer_updated_at,
  gps_distance_at_manual_update = updates.gps_distance_at_manual_update
-- Otherwise, keep the existing data
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------


