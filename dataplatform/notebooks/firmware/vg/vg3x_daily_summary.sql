-- Databricks notebook source

-- This table is used as the basis for nearly all of the tables that plug into
-- the VG health dashboard.
CREATE OR REPLACE TEMP VIEW vg_daily_summary AS (
  SELECT DISTINCT
    db.date,
    db.org_id,
    CASE
        WHEN o.internal_type = 1 THEN "Internal"
        ELSE 'Customer'
    END AS org_type,
    cm.name AS org_name,
    db.device_id,
    db.latest_gateway_id,
    gw.product_id,
    gw.serial,
    gw.variant_id,
    dev.camera_product_id,
    db.active_builds_on_day,
    db.latest_build_on_day,
    COALESCE(ad.trip_count, 0) AS trip_count,
    COALESCE(ad.total_distance, 0) AS total_distance_meters,
    ca.can_bus_type,
    cb.cable_type,
    COALESCE(mod.has_modi, false) as has_modi,
    IF(bax.vg_device_id IS NOT NULL, True, False) as has_baxter,
    IF(octo.octo_deviceid IS NOT NULL, True, False) as has_octo,
    COALESCE(hw.product_version, 'Version 1') AS product_version,
    coalesce(try_cast(rollout_stages.rollout_stage_id AS INT), -1) as rollout_stage_id, -- assign -1 if "override"
    coalesce(try_cast(rollout_stages.product_program_id AS INT), -1) as product_program_id, -- assign -1 if "override"
    rollout_stages.product_program_id_type
  FROM data_analytics.device_builds_program_summary AS db
  LEFT JOIN data_analytics.device_cable_clean AS cb ON
    db.date = cb.date
    AND db.org_id = cb.org_id
    AND db.device_id = cb.device_id
  LEFT JOIN data_analytics.device_canbus_clean AS ca ON
    db.date = ca.date
    AND db.org_id = ca.org_id
    AND db.device_id = ca.device_id
  LEFT JOIN dataprep.active_devices AS ad ON
    db.date = ad.date
    AND db.org_id = ad.org_id
    AND db.device_id = ad.device_id
  LEFT JOIN clouddb.gateways AS gw ON
    db.org_id = gw.org_id
    AND db.latest_gateway_id = gw.id
  LEFT JOIN clouddb.organizations AS o ON
    db.org_id = o.id
  LEFT JOIN dataprep.customer_metadata AS cm ON
    db.org_id = cm.org_id
  LEFT JOIN clouddb.devices AS dev ON
    db.org_id = dev.org_id
    AND db.device_id = dev.id
  LEFT JOIN data_analytics.dataprep_vg_modi_days AS mod ON
    db.date = mod.date
    AND db.org_id = mod.org_id
    AND db.device_id = mod.device_id
  LEFT JOIN data_analytics.vg_hardware_variant_info AS hw ON
    gw.product_id = hw.product_id
    AND db.org_id = hw.org_id
    AND db.device_id = hw.device_id
  LEFT JOIN data_analytics.baxter_devices AS bax ON
    db.date = bax.date
    AND db.org_id = bax.org_id
    AND db.device_id = bax.vg_device_id
    AND bax.date_baxter_first_connected >= db.date
  LEFT JOIN data_analytics.octo_report octo ON
    db.device_id = octo.vg_deviceid
    AND db.org_id = octo.org_id
    AND octo.octo_first_online_date <= db.date
    AND octo.octo_last_online_date >= db.date
  LEFT JOIN dataprep_firmware.gateway_daily_rollout_stages AS rollout_stages ON
    gw.id = rollout_stages.gateway_id
    AND gw.org_id = rollout_stages.org_id
    AND gw.product_id = rollout_stages.product_id
    AND db.date = rollout_stages.date
  WHERE gw.product_id IN (24, 35, 53, 89, 178)
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.vg3x_daily_summary
USING DELTA
PARTITIONED BY (date)
SELECT * FROM vg_daily_summary

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vg_daily_summary_updates AS (
   SELECT DISTINCT * FROM vg_daily_summary
   WHERE date >= date_sub(current_date(),10)
);


MERGE INTO data_analytics.vg3x_daily_summary AS target
USING vg_daily_summary_updates AS updates ON
target.date = updates.date
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
