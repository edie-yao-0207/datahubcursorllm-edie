-- Databricks notebook source

-- COMMAND ----------
create or replace temp view
  codepush_complete_logs as (
    select
      cast (date as date) as days,
      event_type,
      GET_JSON_OBJECT(app_info, '$.releaseType') as releaseType
    from
      datastreams.mobile_logs
    where
      -- TODO: tonia alias these stupid event type names to be smaller
      event_type in ("GLOBAL_CODEPUSH_SUCCEEDED_UPDATE", "GLOBAL_CODEPUSH_FAILED_UPDATE")
      and date >= (current_date() - interval 30 days)
  )


-- COMMAND ----------

-- DBTITLE 1,Codepush Success/Failure Over Time (30 days)
select
  COUNT(*) as count, days, event_type
from
  codepush_complete_logs
GROUP BY event_type, days

-- COMMAND ----------

-- DBTITLE 1,EA Codepush Success/Failure Over Time (30 days)
select
  COUNT(*) as count, days, event_type
from
  codepush_complete_logs
where
  releaseType = "EARLY_ADOPTER"
GROUP BY event_type, days

-- COMMAND ----------

-- DBTITLE 1,PHASE_1 Codepush Success/Failure Over Time (30 days)
select
  COUNT(*) as count, days, event_type
from
  codepush_complete_logs
where
  releaseType = "PHASE_1"
GROUP BY event_type, days

-- COMMAND ----------

-- DBTITLE 1,PHASE_2 Codepush Success/Failure Over Time (30 days)
select
  COUNT(*) as count, days, event_type
from
  codepush_complete_logs
where
  releaseType = "PHASE_2"
GROUP BY event_type, days

-- COMMAND ----------
create or replace temp view
  driver_codepush_build_number as (
    SELECT
    L.driver_id,
    L.org_id,
    codepush_build_number,
    GET_JSON_OBJECT(L.app_info, '$.releaseType') as release_type
    FROM
    datastreams.mobile_logs AS L INNER JOIN datastreams.mobile_device_info AS D
      ON L.device_uuid = D.device_uuid
    WHERE
      bundle_identifier = 'driver'
      AND codepush_build_number != 1
      AND L.date >= (current_date() - interval 1 days)
    AND NOT( L.org_id IN (
    24835, 6091, 37700, 562950000000000, 562950000000000, 18078, 3401, 53837, 35295, 6253, 23050, 562950000000000, 8830, 31609, 39364, 28180, 24210, 9283, 38579, 44304, 40256, 32550, 52115, 22947, 24468, 29129, 16491, 161, 8955, 33028, 21770, 53836, 19417, 26312, 24598, 21211, 811, 16974, 22246, 562950000000000, 36211, 22860, 26910, 1363, 2187, 5120, 26994, 22950, 30375, 35802, 7083, 37331, 20936, 14804, 50157, 51174, 205, 38515, 26781, 33026, 37618, 14742, 42412, 49017, 22945, 562949953423442, 562949953423193, 562949953421385, 562949953423186, 562949953421849, 562949953421349, 562949953421399, 562949953422001, 43356, 14431, 43539, 5289, 28307, 30423, 18096, 32539, 8709,
    23506))
  )

-- COMMAND ----------

-- DBTITLE 1,# of Drivers per Version (1 day)
-- Num drivers per version over last day
SELECT COUNT(*) as count, codepush_build_number
FROM (
  SELECT
  driver_id,
  org_id,
  MAX(codepush_build_number) as codepush_build_number
  FROM
    driver_codepush_build_number
  GROUP BY driver_id, org_id
  ORDER BY codepush_build_number DESC
)
GROUP BY codepush_build_number
ORDER BY count DESC
LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,EA - # of Drivers per Version (1 day)
-- Num EA drivers per version over last day
SELECT COUNT(*) as count, codepush_build_number
FROM (
  SELECT
  driver_id,
  org_id,
  MAX(codepush_build_number) as codepush_build_number
  FROM
    driver_codepush_build_number
  WHERE release_type = "EARLY_ADOPTER"
  GROUP BY driver_id, org_id
  ORDER BY codepush_build_number DESC
)
GROUP BY codepush_build_number
ORDER BY count DESC
LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,PHASE_1 - # of Drivers per Version (1 day)
-- Num PHASE_1 drivers per version over last day
SELECT COUNT(*) as count, codepush_build_number
FROM (
  SELECT
  driver_id,
  org_id,
  MAX(codepush_build_number) as codepush_build_number
  FROM
    driver_codepush_build_number
  WHERE release_type = "PHASE_1"
  GROUP BY driver_id, org_id
  ORDER BY codepush_build_number DESC
)
GROUP BY codepush_build_number
ORDER BY count DESC
LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,PHASE_2 - # of Drivers per Version (1 day)
-- Num PHASE_2 drivers per version over last day
SELECT COUNT(*) as count, codepush_build_number
FROM (
  SELECT
  driver_id,
  org_id,
  MAX(codepush_build_number) as codepush_build_number
  FROM
    driver_codepush_build_number
  WHERE release_type = "PHASE_2"
  GROUP BY driver_id, org_id
  ORDER BY codepush_build_number DESC
)
GROUP BY codepush_build_number
ORDER BY count DESC
LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,# of Drivers per Version (7 days)
-- Num drivers per version over time
SELECT COUNT(*) as count, daydate, codepush_build_number
FROM (
  SELECT
  driver_id,
  org_id,
  MAX(codepush_build_number) as codepush_build_number,
  CAST (L.date AS date) AS daydate
  FROM 
  datastreams.mobile_logs AS L INNER JOIN datastreams.mobile_device_info AS D 
    ON L.device_uuid = D.device_uuid
  WHERE
    bundle_identifier = 'driver'
    AND codepush_build_number != 1
    --  The latest native build was 52.0.6882   
    AND codepush_build_number > 6882
    AND L.date >= (current_date() - interval 7 days)
  AND NOT( L.org_id IN (
  24835, 6091, 37700, 562950000000000, 562950000000000, 18078, 3401, 53837, 35295, 6253, 23050, 562950000000000, 8830, 31609, 39364, 28180, 24210, 9283, 38579, 44304, 40256, 32550, 52115, 22947, 24468, 29129, 16491, 161, 8955, 33028, 21770, 53836, 19417, 26312, 24598, 21211, 811, 16974, 22246, 562950000000000, 36211, 22860, 26910, 1363, 2187, 5120, 26994, 22950, 30375, 35802, 7083, 37331, 20936, 14804, 50157, 51174, 205, 38515, 26781, 33026, 37618, 14742, 42412, 49017, 22945, 562949953423442, 562949953423193, 562949953421385, 562949953423186, 562949953421849, 562949953421349, 562949953421399, 562949953422001, 43356, 14431, 43539, 5289, 28307, 30423, 18096, 32539, 8709,
  23506))
  GROUP BY L.date, L.driver_id, L.org_id
  ORDER BY daydate DESC
)
GROUP BY daydate, codepush_build_number
ORDER BY daydate ASC
