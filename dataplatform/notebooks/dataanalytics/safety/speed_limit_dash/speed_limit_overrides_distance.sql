-- Databricks notebook source

CREATE OR REPLACE TEMP VIEW speed_limit_overrides_distance AS (
  WITH trips AS (
    SELECT ft.date,
          ft.org_id,
          do.org_name,
          do.account_size_segment_name,
          do.account_industry,
          ft.start_state AS state,
          SUM(distance_miles) AS total_distance_miles
    FROM datamodel_telematics.fct_trips ft
    INNER JOIN datamodel_core.dim_devices dd
      ON ft.device_id = dd.device_id
      AND ft.date = dd.date
    INNER JOIN datamodel_core.dim_organizations do
      ON ft.org_id = do.org_id
      AND ft.date = do.date
    WHERE ft.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
      AND dd.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
      AND do.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
      AND dd.product_name LIKE '%VG%'
      AND do.is_paid_safety_customer
    GROUP BY 1,2,3,4,5,6
  ),
  overrides AS (
    SELECT date,
          org_id,
          org_name,
          account_size_segment_name,
          account_industry,
          revgeo_state,
          revgeo_country,
          COUNT(way_id) AS num_overrides
    FROM data_analytics.speed_limit_overrides
    WHERE date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
    GROUP BY 1,2,3,4,5,6,7
  )

  SELECT COALESCE(t.date,o.date) AS date,
        COALESCE(t.org_id,o.org_id) AS org_id,
        COALESCE(t.org_name,o.org_name) AS org_name,
        COALESCE(t.account_size_segment_name,o.account_size_segment_name) AS account_size_segment_name,
        COALESCE(t.account_industry,o.account_industry) AS account_industry,
        COALESCE(t.state,o.revgeo_state) AS state,
        COALESCE(o.revgeo_country, 'Unknown') AS country,
        t.total_distance_miles,
        COALESCE(o.num_overrides,0) AS num_overrides
  FROM trips t
  LEFT JOIN overrides o
    ON t.date = o.date
    AND t.org_id = o.org_id
    AND t.state = o.revgeo_state
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.speed_limit_overrides_distance
USING delta
PARTITIONED BY (date)
AS
SELECT * FROM speed_limit_overrides_distance

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW speed_limit_overrides_distance_updates AS (
  SELECT *
  FROM speed_limit_overrides_distance
  WHERE date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
);

MERGE INTO data_analytics.speed_limit_overrides_distance AS target
USING speed_limit_overrides_distance_updates AS updates
ON target.date = updates.date
AND target.org_id = updates.org_id
AND target.state = updates.state
AND target.country = updates.country
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT * ;
