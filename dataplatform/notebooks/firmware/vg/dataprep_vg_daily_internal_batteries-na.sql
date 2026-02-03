-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW osdbattery AS (
  SELECT
    date,
    org_id,
    object_id,
    count(*) AS voltage_count
  FROM kinesisstats.osdbattery
  WHERE value.is_end = false
    AND value.is_databreak = false
    AND value.int_value > 0
    AND value.int_value <= 3000
    --AND date >= date_sub(current_date(),10)
  GROUP BY 
    date,
    org_id,
    object_id
)

-- COMMAND ----------

-- Assume that internal battery is truly dead on a given day if
-- it records more than 10 battery voltages below 3V.
CREATE OR REPLACE TEMP VIEW dead_batteries AS (
  SELECT
    date,
    org_id,
    object_id,
    'Compromised' AS internal_battery_status
  FROM osdbattery
  WHERE voltage_count > 10
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vg3x_batteries_joined AS (
 SELECT
    a.date,
    a.org_id,
    a.org_name,
    a.org_type,
    a.device_id,
    a.can_bus_type,
    a.product_type,
    a.product_version,
    a.product_program_id,
    a.product_program_id_type,
    a.rollout_stage_id,
    a.status,
    a.latest_build_on_day,
    a.has_modi,
    a.has_baxter,
    coalesce(b.internal_battery_status, 'Healthy') AS internal_battery_status
   FROM data_analytics.dataprep_vg3x_daily_health_metrics AS a
   LEFT JOIN dead_batteries AS b ON
     a.date = b.date 
     AND a.org_id = b.org_id
     AND a.device_id = b.object_id
   --WHERE a.date >= date_sub(current_date(),10)
);

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vg3x_daily_aggregates AS (
  SELECT 
    date,
    org_id,
    org_name,
    org_type,
    can_bus_type,
    product_type,
    product_version,
    product_program_id,
    product_program_id_type,
    rollout_stage_id,
    status,
    latest_build_on_day,
    has_modi,
    has_baxter,
    internal_battery_status,
    count(distinct device_id) AS device_count
  FROM vg3x_batteries_joined
  GROUP BY 
    date,
    org_id,
    org_name,
    org_type,
    can_bus_type,
    product_type,
    product_version,
    product_program_id,
    product_program_id_type,
    rollout_stage_id,
    status,
    latest_build_on_day,
    has_modi,
    has_baxter,
    internal_battery_status
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.dataprep_vg_daily_internal_batteries
USING DELTA
PARTITIONED BY (date)
SELECT * FROM vg3x_daily_aggregates

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_battery_status_updates AS (
  SELECT * FROM vg3x_daily_aggregates 
  WHERE date > date_sub(current_date(),10)
);

MERGE INTO data_analytics.dataprep_vg_daily_internal_batteries AS target
USING device_battery_status_updates AS updates ON
target.date = updates.date
AND target.org_id = updates.org_id
AND target.org_name = updates.org_name
AND target.org_type = updates.org_type
AND target.can_bus_type = updates.can_bus_type
AND target.product_type = updates.product_type
AND target.product_version = updates.product_version
AND target.product_program_id = updates.product_program_id
AND target.product_program_id_type = updates.product_program_id_type
AND target.rollout_stage_id = updates.rollout_stage_id
AND target.status = updates.status
AND target.latest_build_on_day = updates.latest_build_on_day
AND target.has_modi = updates.has_modi
AND target.has_baxter = updates.has_baxter
AND target.internal_battery_status = updates.internal_battery_status
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
