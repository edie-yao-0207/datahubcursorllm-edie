-- Databricks notebook source

CREATE OR REPLACE TEMP VIEW speed_limit_overrides AS (
  SELECT date(timestamp_millis(slo.created_at_ms)) AS date,
        slo.created_at_ms,
        slo.way_id,
        COALESCE(wis.revgeo_state,'Unknown') AS revgeo_state,
        COALESCE(wis.revgeo_country,'Unknown') AS revgeo_country,
        slo.org_id,
        do.org_name,
        do.account_size_segment_name,
        do.account_industry,
        slo.original_speed_limit_milliknots,
        slo.override_speed_limit_milliknots,
        ROUND(slo.original_speed_limit_milliknots * 0.00115078) AS original_speed_limit_mph,
        ROUND(slo.override_speed_limit_milliknots * 0.00115078) AS override_speed_limit_mph
  FROM speedlimitsdb.speed_limit_overrides slo
  LEFT JOIN data_analytics.traveled_ways wis
    ON slo.way_id = wis.way_id
  INNER JOIN datamodel_core.dim_organizations do
    ON slo.org_id = do.org_id
    AND date(timestamp_millis(slo.created_at_ms)) = do.date
  WHERE date(timestamp_millis(slo.created_at_ms)) BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
    AND do.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
    AND do.is_paid_safety_customer
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.speed_limit_overrides
USING delta
PARTITIONED BY (date)
AS
SELECT * FROM speed_limit_overrides

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW speed_limit_overrides_updates AS (
  SELECT *
  FROM speed_limit_overrides
  WHERE date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
);

MERGE INTO data_analytics.speed_limit_overrides AS target
USING speed_limit_overrides_updates AS updates
ON target.date = updates.date
AND target.org_id = updates.org_id
AND target.created_at_ms = updates.created_at_ms
AND target.way_id = updates.way_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT * ;
