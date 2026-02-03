-- Databricks notebook source
-- When looking up values within value.proto_value.system_stats for this objectstat
-- is_start, is_end, is_databreak, and int_value are not useful; there can be non-zero measurements
-- within cm3x_power_stats even when int_value is 0 or is_end, is_databreak is True.
CREATE OR REPLACE TEMPORARY VIEW osdcm3xpowerstats_raw AS (
  SELECT
    c3p.date
    ,c3p.stat_type
    ,c3p.org_id
    ,c3p.object_id
    ,c3p.time
    ,from_unixtime(c3p.time/1000, 'yyyy-MM-dd HH:mm:ss') AS time_human
    ,value.proto_value.cm3x_power_stats.usb_system_power_now_uw
    ,value.proto_value.cm3x_power_stats.battery_voltage_now_uv
    ,dv.product_id
    ,f.name AS product_name
  FROM kinesisstats.osdcm3xpowerstats c3p
  INNER JOIN productsdb.devices dv ON c3p.object_id = dv.id
  INNER JOIN definitions.products f ON dv.product_id = f.product_id
  WHERE dv.product_id IN (155, 167) -- Brigid devices only
    AND `date` BETWEEN coalesce(nullif(getArgument("start_date"), ''), date_sub(CURRENT_DATE(), 6))
    AND coalesce(nullif(getArgument("end_date"), ''), CURRENT_DATE())
);

-- COMMAND ----------

-- Omit entries where the next entry is more than 90s later.
-- We have to make an assumption about which intervals are valid since is_end and is_databreak
-- objectstat fields are invalid for this objectstat.
-- Initial data analysis shows that using a filter of 90s includes >0.984 of the original data points.
CREATE OR REPLACE TEMPORARY VIEW osdcm3xpowerstats_ranges AS (
  WITH a AS (
    SELECT *
      ,lead(`time`) OVER (PARTITION BY org_id, object_id ORDER BY `time`) AS time_next
    FROM osdcm3xpowerstats_raw
  )
  SELECT * FROM a
  WHERE isnotnull(time_next)
    AND (time_next - time) <= 90000
);

-- COMMAND ----------

-- Creates date ranges that will be used to bound objectstat intervals
-- that break over multiple days.
CREATE OR REPLACE TEMPORARY VIEW date_ranges AS (
  WITH a AS (
    SELECT explode( sequence(
        coalesce(nullif(date(getArgument("start_date")), ''), date_sub(CURRENT_DATE(), 6)),
        coalesce(nullif(date(getArgument("end_date")), ''), CURRENT_DATE()),
        INTERVAL 1 DAY)
      ) date
  )
  SELECT string(`date`)
    ,unix_timestamp(`date`)*1000 start_day
    ,(unix_timestamp(`date`)*1000) + (86400*1000 - 1) end_day
  FROM a
);

-- COMMAND ----------

-- Creates multiple entries for objectstat intervals that break over multiple days
CREATE OR REPLACE TEMPORARY VIEW osdcm3xpowerstats_join_dates AS (
  SELECT
    c3pr.*
    ,d.date date_from_ranges
    ,d.start_day
    ,d.end_day
  FROM osdcm3xpowerstats_ranges c3pr
  JOIN date_ranges d ON c3pr.time > d.start_day AND c3pr.time < d.end_day
    OR c3pr.time_next > d.start_day AND c3pr.time_next < d.end_day
  ORDER BY `time`
);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW osdcm3xpowerstats_final AS (
  SELECT
    c3pd.date_from_ranges AS `date`
    ,c3pd.stat_type
    ,c3pd.org_id
    ,c3pd.object_id
    ,greatest(c3pd.time, c3pd.start_day) AS start_ms
    ,from_unixtime(greatest(c3pd.time, c3pd.start_day)/1000, 'yyyy-MM-dd HH:mm:ss') AS start_ms_human
    ,least(c3pd.time_next, c3pd.end_day) AS end_ms
    ,from_unixtime(least(c3pd.time_next, c3pd.end_day)/1000, 'yyyy-MM-dd HH:mm:ss') AS end_ms_human
    ,c3pd.usb_system_power_now_uw
    ,c3pd.battery_voltage_now_uv
    ,c3pd.product_id
    ,c3pd.product_name
  FROM osdcm3xpowerstats_join_dates c3pd
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataengineering.osdcm3xpowerstats_intervals
USING DELTA
PARTITIONED BY (`date`)
COMMENT 'Creates time intervals bounded by `start_ms` and `end_ms` that begin and end within a single UTC day to enable daily aggregation calculations. NOTE: There may be null values in the measurements, since the source kinesisstat does not use the objectstat flags of is_value, is_start, is_end, is_databreak to indicate when valid values are present.'
AS SELECT * FROM osdcm3xpowerstats_final;

-- COMMAND ----------

MERGE INTO dataengineering.osdcm3xpowerstats_intervals AS target
USING osdcm3xpowerstats_final AS source ON target.date = source.date
  AND target.org_id = source.org_id
  AND target.object_id = source.object_id
  AND target.start_ms = source.start_ms
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
