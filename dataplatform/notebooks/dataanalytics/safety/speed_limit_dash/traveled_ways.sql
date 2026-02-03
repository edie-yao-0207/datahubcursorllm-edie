-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW traveled_ways AS (
  WITH base AS (
    SELECT value.way_id,
          value.revgeo_state,
          value.revgeo_country
    FROM kinesisstats.location
    WHERE date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
      AND value.has_way_id = TRUE
      AND value.revgeo_state IS NOT NULL
    GROUP BY 1,2,3
  )

  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (PARTITION BY way_id ORDER BY revgeo_state, revgeo_country) = 1 -- dedup border crossing ways
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.traveled_ways
USING delta
AS
SELECT * FROM traveled_ways

-- COMMAND ----------

MERGE INTO data_analytics.traveled_ways AS target
USING traveled_ways AS updates
ON target.way_id = updates.way_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT * ;

-- COMMAND ----------

