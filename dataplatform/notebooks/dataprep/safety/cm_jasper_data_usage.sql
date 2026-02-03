-- Databricks notebook source
-- Grab the distinct iccids linked reported by each vg id. Opting to reference the distinct reported iccids from
-- osdcellularinterfacestate table rather than productsdb.devices table. The ICCID col in the productsdb.devices table only reflects
-- the last reported ICCID of the devices. This will cause issues for VGs with multiple SIMs
CREATE OR REPLACE TEMP VIEW daily_cm_vg_iccid AS (
  SELECT DISTINCT
    cm_linked_vgs.org_id,
    cm_linked_vgs.vg_device_id as device_id,
    cm_linked_vgs.linked_cm_id as cm_device_id,
    cm_linked_vgs.cm_product_id,
    cell.date,
    cell.value.proto_value.interface_state.cellular.iccid
  FROM dataprep_safety.cm_linked_vgs AS cm_linked_vgs
  LEFT JOIN kinesisstats.osdcellularinterfacestate AS cell ON
    cm_linked_vgs.vg_device_id = cell.object_id
    AND cm_linked_vgs.org_id = cell.org_id
    AND cell.value.is_databreak = 'false'
    AND cell.value.is_end = 'false'
  )

-- COMMAND ----------

-- Join ATT data usage as reported by Jasper. Fill in null values for devices that didn't report a iccid value on a given day. We can omit
-- ICCID values here since we're interested in device level granularity.
CREATE OR REPLACE TEMP VIEW cm_daily_data_usage AS (
  SELECT
    a.date,
    a.org_id,
    a.device_id,
    a.cm_device_id,
    a.cm_product_id,
    COALESCE(SUM(b.data_usage)/1000000000, 0) AS total_att_gb_data_used
  FROM daily_cm_vg_iccid AS a
  LEFT JOIN dataprep_cellular.att_daily_usage AS b ON
    a.iccid = b.iccid
    AND a.date = b.record_open_date
  GROUP BY
    a.date,
    a.org_id,
    a.device_id,
    a.cm_device_id,
    a.cm_product_id
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep_safety.cm_daily_data_usage USING DELTA
PARTITIONED BY (date)
SELECT * FROM cm_daily_data_usage

-- COMMAND ----------
CREATE OR REPLACE TEMP VIEW cm_daily_data_usage_updates AS (
  SELECT *
  FROM cm_daily_data_usage
  where date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
    and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

MERGE INTO dataprep_safety.cm_daily_data_usage AS target
USING cm_daily_data_usage_updates AS updates ON
target.date = updates.date
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
AND target.cm_device_id = updates.cm_device_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
-- COMMAND ----------


