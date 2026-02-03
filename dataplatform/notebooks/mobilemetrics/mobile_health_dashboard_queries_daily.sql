-- Databricks notebook source
-- MAGIC %md 
-- MAGIC *** This notebook is run once daily with a timeout of 6 hours. If a query times out, cells below it will not run.
-- MAGIC  Expensive queries should be placed in the mobile_health_dashboard_queries_expensive notebook which runs once every few days ***

-- COMMAND ----------

create or replace temp view
  last_fourteen_days as (
    select
      l.org_id as orgId,
      l.device_uuid as uuid,
      cast (l.date as date) AS days,
      GET_JSON_OBJECT(l.app_info, '$.releaseType') as releaseType,
      event_type
    from
      datastreams.mobile_logs l
      inner join datastreams.mobile_device_info d ON l.device_uuid = d.device_uuid
    where
      d.bundle_identifier = 'driver'
      and l.date >= (current_date() - interval 14 days)
  )

-- COMMAND ----------

-- DBTITLE 1,Daily Active Users
with 
  unique_devices AS (
    select 
      days,
      releaseType
    from 
      last_fourteen_days
    group by uuid, days, releaseType
)

select days, releaseType, count(*) as count from unique_devices group by days, releaseType order by days desc


-- COMMAND ----------

-- DBTITLE 1,Daily Active Orgs
with 
  unique_orgs AS (
    select 
      days,
      releaseType
    from 
      last_fourteen_days
    group by orgId, days, releaseType
)

select days, releaseType, count(*) as count from unique_orgs group by days, releaseType order by days desc

-- COMMAND ----------

-- DBTITLE 1,D2 Lagginess by Route (7 days)
select
  GET_JSON_OBJECT(json_params, "$.currentRouteId") AS routeId,
  percentile_approx(CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double), 0.5) AS p50,
  percentile_approx(CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double), 0.9) AS p90, 
  percentile_approx(CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double), 0.95) AS p95, 
  percentile_approx(CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double), 0.99) AS p99, 
  COUNT(1) AS Samples
from
  datastreams.mobile_logs 
where
  date >= (current_date() - interval 7 days)
  and event_type LIKE 'GLOBAL_REPORT_PERF_MEASUREMENT' 
  and driver_id IS NOT NULL 
  and CAST(GET_JSON_OBJECT(json_params, "$.delayMs") AS double) < 1000 * 60 * 10
group by
  routeId
having count(1)>1000
order by p90 desc
limit 100

-- COMMAND ----------

-- DBTITLE 1,Driver Sentiment (D1 + D2)
with 
  groupedRatings as (
select
  COUNT(distinct driver_id) as count,
  COUNT(if (action="like", true, null)) as likes,
  COUNT(if (action="dislike", true, null)) as dislikes,
  ROUND(100 * ((COUNT(if (action="like", true, null)) - COUNT(if (action="dislike", true, null))) / (COUNT(if (action="like", true, null)) + COUNT(if (action="dislike", true, null))))) as Sentiment,
  cast (created_at as date) as createdAtDate,
  unix_timestamp(TIMESTAMP(cast (created_at as date))) as unixCreatedAt
from
  clouddb.driver_rating_actions 
where
  action in ("like", "dislike")
  and date >= (current_date() - interval 60 days)
group by
  createdAtDate
)

select 
  count,
  createdAtDate,
  likes,
  dislikes,
  Sentiment,
  AVG(Sentiment) over(order by unixCreatedAt range between 604800 preceding and current row) AS sentiment_rolling_avg_7_days,
  AVG(Sentiment) over(order by unixCreatedAt range between 2592000 preceding and current row) AS sentiment_rolling_avg_30_days
from 
  groupedRatings
order by
   createdAtDate desc

-- COMMAND ----------

create or replace temp view 
  clock_data as (
    select
      cast (GET_JSON_OBJECT(json_params, "$.localNowMs") as double) as localNowMs, 
      cast (GET_JSON_OBJECT(json_params, "$.syncedNowMs") as double) as syncedNowMs,
      cast (GET_JSON_OBJECT(json_params, "$.sourceNowMs") as double) as sourceNowMs,
      GET_JSON_OBJECT(json_params, "$.source") as clockSource,
      timestamp
    from 
      datastreams.mobile_logs
    where
      event_type = "DRIVER_LOG_ABOUT_SYNCED_CLOCK"
       and date >= (current_date() - interval 30 days)
  )

-- COMMAND ----------

-- DBTITLE 1,Relative Variation Between Local Clock and Synced Clock
select 
  timestamp,
  clockSource,
  sourceNowMs,
  syncedNowMs,
  localNowMs,
  abs(sourceNowMs - syncedNowMs) as correction, -- this represents how close our calculated sync clock is to the source clock (either vg or backend)
  abs(sourceNowMs - localNowMs) as skew, -- this represents the clock skew (the amount of absolute time between the local device clock and the source clock)
  
  -- correction should be a small value relative to skew. In other words, if/when we detect a clock skew our corrected clock should be closer in value to the source
  -- correctionMinusSkewSeconds should therefore be less than or equal to zero. Otherwise our corrected clock is futher from the source clock than the local device clock
  ROUND((abs(clock_data.sourceNowMs - clock_data.syncedNowMs) - abs(clock_data.sourceNowMs - clock_data.localNowMs)) / 1000, 1) as correctionMinusSkewSeconds  
from clock_data

-- COMMAND ----------

-- DBTITLE 1,Daily Driver App Data Usage Per Device
select 
  date,
  bytes_driver_app_traffic / device_count as bytes_driver_app_traffic_per_device
from (
  select
    date,
    sum(value.int_value) as bytes_driver_app_traffic,
    count(distinct(object_id)) as device_count
  from
    kinesisstats.osdwifiapwhitelistbytes
  where
    date > date_sub(current_date(), 180)
    and date <= current_date()
    and date != "2020-07-30" -- crazy outlier so vast you can't even see the rest of the graph; must be a bug
    and org_id != 1 -- org 1 is "unassigned devices", and the cloud team will make them stop using data soon
  group by date
  order by date asc
)

-- COMMAND ----------

-- DBTITLE 1,Daily Non Driver App Data Usage Per Device
select 
  date,
  bytes_driver_app_traffic / device_count as bytes_driver_app_traffic_per_device
from (
  select
    date,
    sum(value.int_value) as bytes_driver_app_traffic,
    count(distinct(object_id)) as device_count
  from
    kinesisstats.osdwifiapbytes
  where
    date > date_sub(current_date(), 180)
    and date <= current_date()
    and date != "2020-07-30" -- crazy outlier so vast you can't even see the rest of the graph; must be a bug
    and org_id != 1 -- org 1 is "unassigned devices", and the cloud team will make them stop using data soon
  group by date
  order by date asc
)

-- COMMAND ----------

-- DBTITLE 1,Last 60 Days of Logs (for quicker queries?)
create or replace temp view 
  last_sixty_days as (
    select *
    from datastreams.mobile_logs
    where date >= (current_date() - interval 60 days)
  )

-- COMMAND ----------

-- DBTITLE 1,Throttled Log Events over Time
select
  COUNT(*) as count,
  days,
  releaseType
from
  last_fourteen_days
where event_type = "GLOBAL_REMOTE_META_LOG_EVENT_THROTTLED"
group by days, releaseType

-- COMMAND ----------

-- DBTITLE 1,Skipped Log Events over Time
select
  COUNT(*) as count,
  days,
  releaseType
from
  last_fourteen_days
where event_type = "GLOBAL_SKIPPED_LOGS"
group by days, releaseType

-- COMMAND ----------

-- DBTITLE 1,Missing Data Events over Time
select
  COUNT(*) as count,
  days,
  releaseType
from
  last_fourteen_days
where event_type = "DRIVER_VG_DIRECT_CONNECTION_LOG_MISSED_DATA"
group by days, releaseType

-- COMMAND ----------

select 
  os_version, platform, count(*)
from 
     datastreams.mobile_device_info 
where 
  datastreams.mobile_device_info.bundle_identifier = 'driver'
  and datastreams.mobile_device_info.date >= (current_date() - interval 14 days)
group by os_version, platform


-- COMMAND ----------

select * from datastreams.mobile_logs
   where date >= (current_date() - interval 1 days) and event_type = "GLOBAL_LOG_FROM_ERROR_BOUNDARY"

-- COMMAND ----------

-- DBTITLE 1,# of Drivers per Version (1 day)
-- https://samsara-dev-us-west-2.cloud.databricks.com/?o=5924096274798303#notebook/1853859818484864/command/1853859818484867

-- COMMAND ----------

-- MAGIC %md Startup time (shipping next native merge window) (lowercase driver is Android, uppercase Driver is iOS)

-- COMMAND ----------

-- perhaps filter negatives out before calculation, apparently possible but unlikely for Android to get negative values
select GET_JSON_OBJECT(json_params, "$.bundleId"),  date, percentile_approx(CAST(GET_JSON_OBJECT(json_params, "$.startupTimeMs") AS double), 0.9) AS p90ms from datastreams.mobile_logs
   where date >= (current_date() - interval 7 days) and event_type = "GLOBAL_GOT_STARTUP_TIME" and GET_JSON_OBJECT(json_params, "$.bundleId") like "%river" 
     and GET_JSON_OBJECT(json_params, "$.isDev") = false 
   group by GET_JSON_OBJECT(json_params, "$.bundleId"), date

