-- Databricks notebook source
-- When looking up values within value.proto_value.system_stats for this objectstat
-- is_start, is_end, is_databreak, and int_value are not useful; there can be non-zero measurements
-- within cm3x_tsens even when int_value is 0 or is_end, is_databreak is True.
CREATE OR REPLACE TEMPORARY VIEW osdcm3xthermalsensors_raw AS (
  SELECT
    c3t.date
    ,c3t.stat_type
    ,c3t.org_id
    ,c3t.object_id
    ,c3t.time
    ,from_unixtime(c3t.time/1000, 'yyyy-MM-dd HH:mm:ss') AS time_human
    ,value.proto_value.cm3x_tsens.soc_temp_milli_degree_c
    ,value.proto_value.cm3x_tsens.emmc_temp_milli_degree_c
    ,value.proto_value.cm3x_tsens.road_cam_temp_milli_degree_c
    ,value.proto_value.cm3x_tsens.driver_cam_temp_milli_degree_c
    ,dv.product_id
    ,f.name AS product_name
  FROM kinesisstats.osdcm3xthermalsensors c3t
  INNER JOIN productsdb.devices dv ON c3t.object_id = dv.id
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
CREATE OR REPLACE TEMPORARY VIEW osdcm3xthermalsensors_ranges AS (
  WITH a AS (
    SELECT *
      ,lead(`time`) OVER (PARTITION BY org_id, object_id ORDER BY `time`) AS time_next
    FROM osdcm3xthermalsensors_raw
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
CREATE OR REPLACE TEMPORARY VIEW osdcm3xthermalsensors_join_dates AS (
  SELECT
    c3tr.*
    ,d.date date_from_ranges
    ,d.start_day
    ,d.end_day
  FROM osdcm3xthermalsensors_ranges c3tr
  JOIN date_ranges d ON c3tr.time > d.start_day AND c3tr.time < d.end_day
    OR c3tr.time_next > d.start_day AND c3tr.time_next < d.end_day
  ORDER BY `time`
);

-- COMMAND ----------

-- Creates new objectstat intervals with start_ms and end_ms falling within a single UTC day
CREATE OR REPLACE TEMPORARY VIEW osdcm3xthermalsensors_final AS (
  SELECT
    c3td.date_from_ranges AS `date`
    ,c3td.stat_type
    ,c3td.org_id
    ,c3td.object_id
    ,greatest(c3td.time, c3td.start_day) AS start_ms
    ,from_unixtime(greatest(c3td.time, c3td.start_day)/1000, 'yyyy-MM-dd HH:mm:ss') AS start_ms_human
    ,least(c3td.time_next, c3td.end_day) AS end_ms
    ,from_unixtime(least(c3td.time_next, c3td.end_day)/1000, 'yyyy-MM-dd HH:mm:ss') AS end_ms_human
    ,c3td.soc_temp_milli_degree_c
    ,c3td.emmc_temp_milli_degree_c
    ,c3td.road_cam_temp_milli_degree_c
    ,c3td.driver_cam_temp_milli_degree_c
    ,c3td.product_id
    ,c3td.product_name
  FROM osdcm3xthermalsensors_join_dates c3td
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataengineering.osdcm3xthermalsensors_intervals
USING DELTA
PARTITIONED BY (`date`)
COMMENT 'Creates time intervals bounded by `start_ms` and `end_ms` that begin and end within a single UTC day to enable daily aggregation calculations. NOTE: There may be null values in the measurements, since the source kinesisstat does not use the objectstat flags of is_value, is_start, is_end, is_databreak to indicate when valid values are present.'
AS SELECT * FROM osdcm3xthermalsensors_final;

-- COMMAND ----------

MERGE INTO dataengineering.osdcm3xthermalsensors_intervals AS target
USING osdcm3xthermalsensors_final AS source ON target.date = source.date
  AND target.org_id = source.org_id
  AND target.object_id = source.object_id
  AND target.start_ms = source.start_ms
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
