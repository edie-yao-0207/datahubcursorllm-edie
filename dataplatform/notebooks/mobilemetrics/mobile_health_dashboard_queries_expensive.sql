-- Databricks notebook source
-- MAGIC %md *** This notebook will run on a job every few days. Place any queries that take over an hour here instead of the daily mobile health dashboard ***

-- COMMAND ----------

-- DBTITLE 1,D2 Lagginess 180 days
select
    date AS Day,
    percentile_approx(CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double), 0.5) AS p50,
    COUNT(1) AS Samples, 
    percentile_approx(CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double), 0.9) AS p90, 
    percentile_approx(CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double), 0.95) AS p95, 
    percentile_approx(CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double), 0.96) AS p96, 
    percentile_approx(CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double), 0.97) AS p97, 
    percentile_approx(CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double), 0.98) AS p98, 
    percentile_approx(CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double), 0.99) AS p99 
  from
    datastreams.mobile_logs
  where
    date >= (current_date() - interval 180 days)
    and event_type = 'GLOBAL_REPORT_PERF_MEASUREMENT' 
    and driver_id IS NOT NULL 
    and CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double) < 1000 * 60 * 10
  group by Day 
  order by Day desc

-- COMMAND ----------

-- DBTITLE 1,Last 60 days of Logs
create or replace temp view 
  last_sixty_days as (
    select *
    from datastreams.mobile_logs
    where date >= (current_date() - interval 60 days)
  )

-- COMMAND ----------

-- DBTITLE 1,Last 60 days of Log: external orgs only
create or replace temp view 
  last_sixty_days_no_internal as (
    select *
    from last_sixty_days l
    LEFT JOIN clouddb.organizations o
    ON l.org_id = o.id
    WHERE o.internal_type = 0 -- exclude internal orgs
    AND l.driver_id IS NOT NULL
  )

-- COMMAND ----------

-- DBTITLE 1,# of Drivers Who Uninstalled
create or replace temp view 
  reinstalls_by_device_model as (
  SELECT 
    CAST (last_sixty_days_no_internal.date AS date) AS days,
    datastreams.mobile_device_info.model as deviceModel,
    last_sixty_days_no_internal.driver_id, COUNT(distinct last_sixty_days_no_internal.device_uuid) as reinstall
    FROM last_sixty_days_no_internal
    INNER JOIN datastreams.mobile_device_info on last_sixty_days_no_internal.device_uuid = datastreams.mobile_device_info.device_uuid
    GROUP BY last_sixty_days_no_internal.driver_id, days, deviceModel
    HAVING reinstall > 2
)

-- COMMAND ----------

-- DBTITLE 1,# of Drivers who Uninstalled by Days
select days, count(*) as reinstallsByDay
from reinstalls_by_device_model
GROUP BY days
ORDER BY days desc

-- COMMAND ----------


