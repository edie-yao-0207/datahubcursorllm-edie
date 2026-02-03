-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW speed_incab_alerts AS (
  WITH base AS (
    SELECT a.date,
          a.org_id,
          org.org_name,
          org.account_size_segment_name,
          org.account_industry,
          dev.associated_devices.vg_device_id as vg_device_id,
          a.time AS alerted_at_ms,
          a.value.proto_value.cm3x_audio_alert_info.trigger_time_ms
    FROM kinesisstats.osdcm3xaudioalertinfo  a -- extract alerts data
    INNER JOIN datamodel_core.dim_organizations org
      ON a.date = org.date
      AND a.org_id = org.org_id
    INNER JOIN datamodel_core.dim_devices dev
      ON a.date = dev.date
      AND a.object_id = dev.device_id
    WHERE a.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1)) -- get relavant data
      AND org.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
      AND dev.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
      AND org.is_paid_safety_customer
      AND a.value.proto_value.cm3x_audio_alert_info.event_type = 5 -- SPEEDING_ALERT_EVENT
    GROUP BY 1,2,3,4,5,6,7,8
  ),
-- https://github.com/samsara-dev/backend/blob/55abb8db613264363e96a53e8d1ea2c69476515d/go/src/samsaradev.io/hubproto/safety.proto#L19

  loc AS (
    SELECT b.date,
          b.org_id,
          b.org_name,
          b.account_size_segment_name,
          b.account_industry,
          b.vg_device_id,
          b.alerted_at_ms,
          b.trigger_time_ms,
          abs(loc.time - b.alerted_at_ms) AS time_from_alert,
          loc.value.revgeo_state,
          loc.value.revgeo_country
    FROM base b
    LEFT JOIN kinesisstats.location loc
      ON b.date = loc.date
      AND b.vg_device_id = loc.device_id
      AND loc.time BETWEEN (b.alerted_at_ms - 10000) AND (b.alerted_at_ms + 10000) -- location stat within 10 seconds of alert
  )

  SELECT * FROM loc
  QUALIFY ROW_NUMBER() OVER(PARTITION BY vg_device_id, alerted_at_ms ORDER BY time_from_alert ASC) = 1 -- closest location stat to alert
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.speed_incab_alerts
USING delta
PARTITIONED BY (date)
AS
SELECT * FROM speed_incab_alerts

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW speed_incab_alerts_updates AS (
  SELECT *
  FROM speed_incab_alerts
  WHERE date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
);

MERGE INTO data_analytics.speed_incab_alerts AS target
USING speed_incab_alerts_updates AS updates
ON target.date = updates.date
AND target.org_id = updates.org_id
AND target.vg_device_id = updates.vg_device_id
AND target.alerted_at_ms = updates.alerted_at_ms
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT * ;

-- COMMAND ----------

