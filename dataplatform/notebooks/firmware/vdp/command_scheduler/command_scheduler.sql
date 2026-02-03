-- Databricks notebook source
-- MAGIC  %python
-- MAGIC
-- MAGIC # TODO
-- MAGIC # add a runbook for these  metrics. Note to avoid updating over holidays
-- MAGIC # Need to enable diff'ing the data over time. Should be possible daily
-- MAGIC
-- MAGIC # Future considerations
-- MAGIC # For holidays, we need to check the seasonality of data
-- MAGIC # Add priority weighting to scoreing (EV, US, EMEA unique). Need to make this unique per GEO. Need Federica to provide spec
-- MAGIC # Define preferred cable and use this in the data model, then migrate CST/CET after this
-- MAGIC # Supporting CSV uploads and crosscheck coverage
-- MAGIC
-- MAGIC from datetime import datetime, timedelta
-- MAGIC
-- MAGIC START = "date_start"
-- MAGIC END = "date_end"
-- MAGIC
-- MAGIC attrs = [
-- MAGIC     END,
-- MAGIC     START,
-- MAGIC ]
-- MAGIC
-- MAGIC onedayprev = (datetime.now().replace(
-- MAGIC       hour=0, minute=0, second=0, microsecond=0
-- MAGIC ) - timedelta(days=1)).strftime('%Y-%m-%d')
-- MAGIC
-- MAGIC defaults = {
-- MAGIC   START: onedayprev,
-- MAGIC   END: onedayprev,
-- MAGIC }
-- MAGIC
-- MAGIC def get_argument_from_task_or_widget(key):
-- MAGIC     try:
-- MAGIC       arg = dbutils.jobs.taskValues.get(taskKey="entrypoint", key=key)
-- MAGIC       if arg != "":
-- MAGIC         print("jobrunner set key", key, arg)
-- MAGIC         return arg
-- MAGIC     except Exception as _:
-- MAGIC         _
-- MAGIC
-- MAGIC     try:
-- MAGIC         arg = getArgument(key)
-- MAGIC         if arg != "":
-- MAGIC           print("received argument", key, arg)
-- MAGIC           return arg
-- MAGIC     except Exception as _:
-- MAGIC         _
-- MAGIC
-- MAGIC     print("selecting default for", key, defaults.get(key))
-- MAGIC     return defaults.get(key)
-- MAGIC
-- MAGIC
-- MAGIC def create_property_map():
-- MAGIC     return {key: get_argument_from_task_or_widget(key) for key in attrs}
-- MAGIC
-- MAGIC def set_unquoted_properties(properties):
-- MAGIC     """
-- MAGIC   Settings that do not require quotes (ie code or numbers)
-- MAGIC   """
-- MAGIC     for key in attrs:
-- MAGIC         spark.sql("set vdp.%s = date(\"%s\")" % (key, properties[key]))
-- MAGIC
-- MAGIC PROPERTY_MAP = create_property_map()
-- MAGIC set_unquoted_properties(PROPERTY_MAP)
-- MAGIC
-- MAGIC spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
-- MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "True")

-- COMMAND ----------

set vdp.grain = "1 day"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #firmware_dev.normalized_command_scheduler_diagnostics

-- COMMAND ----------

create table if not exists firmware_dev.normalized_command_scheduler_diagnostics (
  date string not null
  , org_id bigint not null
  , device_id bigint not null
) partitioned by (date)

-- COMMAND ----------

INSERT OVERWRITE firmware_dev.normalized_command_scheduler_diagnostics (
  WITH
    data AS (
      SELECT *
      FROM kinesisstats.osDCommandSchedulerStats
      WHERE date BETWEEN ${vdp.date_start} AND ${vdp.date_end}
    )

  SELECT
    date,
    org_id,
    object_id AS device_id,
    hex(element.command.request_id) AS request_id,
    hex(element.command.response_id) AS response_id,
    hex(element.command.data_identifier) AS data_identifier,
    COALESCE(SUM(element.request_response_stat.request_count), 0) AS request_count,
    COALESCE(SUM(element.request_response_stat.any_response_count), 0) AS any_response_count,
    COALESCE(SUM(element.derated_stat.latest_derate_offset_ms), 0) AS total_derate_offset_ms,
    COALESCE(SUM(element.derated_stat.overall_derate_instances), 0) AS total_derate_instances,
    value.proto_value.command_scheduler_stats.bus_id AS bus_id
  FROM data
  LATERAL VIEW EXPLODE(value.proto_value.command_scheduler_stats.command_stats) AS element
  GROUP BY
    date,
    org_id,
    object_id,
    hex(element.command.request_id),
    hex(element.command.response_id),
    hex(element.command.data_identifier),
    value.proto_value.command_scheduler_stats.bus_id
)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #command_scheduler_stats

-- COMMAND ----------

create table if not exists firmware_dev.command_scheduler_stats (
  date string not null
  , grouping_id bigint not null
  , org_id bigint
  , device_id bigint
) partitioned by (date)

-- COMMAND ----------

insert overwrite firmware_dev.command_scheduler_stats (
  select
    a.date
    , grouping_id(
      a.date
      , a.data_identifier
      , a.org_id
      , a.device_id
      , b.make
      , b.model
      , b.year
      , b.region
      , b.simple_fuel_type
      , b.derived_product_name
    ) as grouping_id
    , a.org_id
    , a.device_id
    , a.data_identifier
    , b.make
    , b.model
    , b.year
    , b.region
    , b.simple_fuel_type as fuel_type
    , sum(a.request_count) as request_count
    , sum(a.any_response_count) as any_response_count
    , (sum(a.any_response_count) / sum(a.request_count)) * 100 AS rolling_sum_response_percentage
    , sum(a.total_derate_offset_ms) as total_derate_offset_ms
    , sum(a.total_derate_instances) as total_derate_instances
    , b.derived_product_name
    , count(*) as count
    , count(DISTINCT a.device_id) as device_count
  from firmware_dev.normalized_command_scheduler_diagnostics AS a
  join firmware_dev.dim_daily_devices AS b
    ON
      a.date = b.date
      and a.org_id = b.org_id
      and a.device_id = b.device_id
GROUP BY
  a.date
  , a.data_identifier
  , grouping sets (
    ( --1
      a.org_id
      , a.device_id
      , b.make
      , b.model
      , b.year
      , b.region
      , b.simple_fuel_type
    )
    , ( --193
      b.make
      , b.model
      , b.year
      , b.region
      , b.simple_fuel_type
    )
    , ( -- 192
      b.make
      , b.model
      , b.year
      , b.region
      , b.simple_fuel_type
      , b.derived_product_name
    )
  )
)

-- COMMAND ----------

-- DBTITLE 1,Response % By Label, All Data Identifiers
-- select
--   concat_ws(" ", array(region, make, model, year, fuel_type)) as label
--   , device_count
--   , data_identifier
--   , rolling_sum_response_percentage
-- from firmware_dev.agg_command_scheduler_stats
-- where
--   make = "FORD"
--   -- Region + MMYF
--   and grouping_id = 193
--   -- There are some outliers with J1939 broadcast vs request rate (over 25000%)
--   and rolling_sum_response_percentage <= 100
-- order by
--   rolling_sum_response_percentage asc

-- -- Analytics intake for this item
-- -- TODO extend promotions table to include data identifier. This would allow automatic annotation of promotions items below

-- COMMAND ----------

-- DBTITLE 1,Ford Global Promotions Only - High Level Screen (No Stats)
-- select
--   concat_ws(" ", array(region, make, model, year, fuel_type)) as label
--   , device_count
--   , data_identifier
--   , rolling_sum_response_percentage
--   , case
--     when rolling_sum_response_percentage >= 10 then 'Yes'
--     else 'No'
--   end as response_category
-- from firmware_dev.agg_command_scheduler_stats
-- where
--   make = "FORD"
--   -- Region + MMYF
--   and grouping_id = 193
--   -- There are some outliers with J1939 broadcast vs request rate (over 25000%)
--   and rolling_sum_response_percentage <= 100
--   -- Not including 222814, 222815, 222816 because their response is assumed to be the same as 222813
--   and (data_identifier = "22582B" or data_identifier = "22404C" or data_identifier = "222813" or data_identifier = "226182" or data_identifier = "22DD01")
-- order by
--   rolling_sum_response_percentage asc

-- -- Analytics intake for this item
-- -- TODO extend promotions table to include data identifier. This would allow automatic annotation of promotions items below

-- COMMAND ----------

-- DBTITLE 1,Standard Odometer Stats Coverage
-- select
--   concat_ws(" ", array(region, make, model, year, fuel_type)) as label
--   , make
--   , model
--   , year
--   , fuel_type
--   , org_id
--   , device_id
--   --, device_count
--   , data_identifier
--   , rolling_sum_response_percentage
--   , case
--     when rolling_sum_response_percentage >= 10 then 'Yes'
--     else 'No'
--   end as response_category
-- from firmware_dev.agg_command_scheduler_stats
-- where
--   make = "FORD"
--   -- Region + MMYF
--   --and grouping_id = 193
--   -- There are some outliers with J1939 broadcast vs request rate (over 25000%)
--   and rolling_sum_response_percentage <= 100
--   and (data_identifier = "1A6" or data_identifier = "22F4A6")
--   and device_id is NOT NULL

-- group by all

-- order by
--   rolling_sum_response_percentage asc




-- COMMAND ----------

-- SELECT
--   make,
--   model,
--   year,
--   fuel_type,
--   COUNT(*) AS message_count,
--   data_identifier,
--   AVG(rolling_sum_response_percentage) AS avg_response_percentage,
--   CASE
--     WHEN AVG(rolling_sum_response_percentage) >= 10 THEN 'Yes'
--     ELSE 'No'
--   END AS response_category
-- FROM firmware_dev.agg_command_scheduler_stats
-- WHERE
--   make = 'FORD'
--   -- Region + MMYF
--   -- AND grouping_id = 193
--   -- There are some outliers with J1939 broadcast vs request rate (over 25000%)
-- --  AND rolling_sum_response_percentage <= 100
--   AND (data_identifier = '1A6' OR data_identifier = '22F4A6')
--   AND device_id IS NOT NULL
--   AND (region = 'US' OR region = 'CA')
-- GROUP BY
--   make,
--   model,
--   year,
--   fuel_type,
--   data_identifier
-- --ORDER BY
-- --  device_count DESC;


-- COMMAND ----------

-- DBTITLE 1,Final Stats for Promotion Screening- Jan to Review
-- WITH device_level_data AS (
--   SELECT
--     device_id,
--     make,
--     model,
--     year,
--     data_identifier,
--     fuel_type,
--     region,
--     AVG(rolling_sum_response_percentage) AS avg_rolling_sum_response_percentage
--   FROM
--     firmware_dev.normalized_command_scheduler_diagnostics
--   WHERE
--     make = "FORD"
--     AND model IS NOT NULL
--     AND year IS NOT NULL
--     AND data_identifier IN ("22582B", "22404C", "222813", "226182", "22DD01")
--     AND grouping_id = 1
--   GROUP BY
--     device_id, make, model, year, data_identifier, fuel_type, region
-- )
-- SELECT
--   CONCAT_WS(" ", array(region, make, model, year, fuel_type)) AS label,
--   make,
--   model,
--   year,
--   data_identifier,
--   COUNT(DISTINCT(device_id)) AS count,
--   PERCENTILE_APPROX(avg_rolling_sum_response_percentage, array(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)) AS pct,
--   MEDIAN(avg_rolling_sum_response_percentage) AS median,
--   STDDEV(avg_rolling_sum_response_percentage) AS stddev,
--   AVG(avg_rolling_sum_response_percentage) AS average,
--   MAX(avg_rolling_sum_response_percentage) AS max_value,
--   MIN(avg_rolling_sum_response_percentage) AS min_value,
--   CASE
--     WHEN average IS NULL THEN 'No Response'
--     WHEN average < 10 AND average IS NOT NULL THEN 'Low Response'
--     ELSE 'Response'
--   END AS low_no_response_category,
-- CASE
--     WHEN (average IS NOT NULL AND stddev = 0)  THEN "Single Value"
--     WHEN stddev is null THEN "No Value"
--     ELSE "Not Single Value"
-- END AS single_value,
--   CASE
--     WHEN max_value = average
--     --1% range
--         OR (max_value BETWEEN (average - 0.01 * average) AND (average + 0.01 * average))
--     THEN "Likely Outlier"
--     ELSE "Not Likely Outlier"
-- END AS likely_outlier,
--   CASE
--     WHEN low_no_response_category = 'Response' AND single_value = "Not Single Value" AND likely_outlier = "Not Likely Outlier" THEN "Yes"
--     ELSE "No"
--   END AS Good_Promotion
-- FROM
--   device_level_data
-- GROUP BY
--   fuel_type, label, data_identifier, make, model, year, region
-- ORDER BY
--   average ASC;

-- COMMAND ----------

-- DBTITLE 1,Old - deprecated by 16
-- select
--  concat_ws(" ", array(region, make, model, year, fuel_type)) as label
--  , make
--  , model
--  , year
--  , data_identifier
--  , count (distinct(device_id)) as count
--  , PERCENTILE_APPROX(rolling_sum_response_percentage, array(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)) AS pct
--  , median(rolling_sum_response_percentage) as median
--  , stddev(rolling_sum_response_percentage) as stddev
--  , avg(rolling_sum_response_percentage) as average
--  --Not sure how relevant these statistical measures are as response distributions do not seem to be normally distributed
--  --, (MEDIAN(rolling_sum_response_percentage) - 2 * STDDEV(rolling_sum_response_percentage)) AS median_minus_2_sd
--  --, (MEDIAN(rolling_sum_response_percentage) + 2 * STDDEV(rolling_sum_response_percentage)) AS median_plus_2_sd
--  , MAX(rolling_sum_response_percentage) as max_value
--  , MIN(rolling_sum_response_percentage) as min_value
-- , CASE
--     WHEN average IS NULL THEN 'No response'
--     WHEN average < 10 and average is not null THEN 'Low Response'
--     ELSE 'Response'
-- END AS low_no_response_category
--  , CASE
-- when stddev = 0 then "Single Value"
-- else "Not single value"
-- END AS single_value
-- , CASE
-- when max_value = average then "Likely Outlier"
-- else "Not Likely Outlier"
-- END AS likely_outlier
-- , case
-- when low_no_response_category = 'Response' and single_value = "Not single value" and likely_outlier = "Not Likely Outlier"
-- then "Yes"
-- else "No"
-- END AS Good_Promotion
-- from
-- firmware_dev.agg_command_scheduler_stats
-- where
--  make = "FORD" AND model is not null and year is not null
--  -- Not including 222814, 222815, 222816 because their response is assumed to be the same as 222813
--   and (data_identifier = "22582B" or data_identifier = "22404C" or data_identifier = "222813" or data_identifier = "226182")
--   -- Region + MMYF
--  and grouping_id = 1
-- group by
-- fuel_type,
-- label,
-- data_identifier,
-- make,
-- model,
-- year,
-- region

-- order by average asc;





-- -- TODO extend promotions table to include data identifier. This would allow automatic annotation of promotions items below

-- COMMAND ----------

-- DBTITLE 1,Stats only view
-- select
--  concat_ws(" ", array(region, make, model, year, fuel_type)) as label
--  , data_identifier
--  , count (distinct(org_id, device_id)) as count
--  , PERCENTILE_APPROX(rolling_sum_response_percentage, array(0.01, 0.99)) AS pct
--  , median(rolling_sum_response_percentage) as median
--  , stddev(rolling_sum_response_percentage) as stddev
--  , avg(rolling_sum_response_percentage) as average
-- from
-- firmware_dev.agg_command_scheduler_stats
-- where
--  make = "FORD" AND model is not null and year is not null
--  and data_identifier = "22582B"
--  and rolling_sum_response_percentage is null
--   -- Region + MMYF
--  and grouping_id = 1
-- group by
-- fuel_type,
-- label,
-- data_identifier,
-- make,
-- model,
-- year,
-- region;



-- COMMAND ----------

-- Ideas to build confidence
--
-- Statistical monitoring, outliers in command rates are out of band (3 STDDEVs above median)
-- This is the X-bar plotting to see if an outlier is detected. Works over any of the groupings above.
-- https://samsara-dev-eu-west-1.cloud.databricks.com/?o=6992178240159315#notebook/1107906877419474/command/1107906877419475
--
-- Coverage dashbaord for specific cohort
-- Build a coverage dashboard for the specific cohort we are release vehicles to, also possible to automate alerting with the above

-- Gaps for getting something into GA for de-rating
-- * Daily refresh of pipeline
-- * LD FF Management
-- * Support for cohort of LD applied devices or a static list of device IDs

-- COMMAND ----------

-- describe firmware_dev.normalized_command_scheduler_diagnostics

-- COMMAND ----------

-- SELECT
--   *
-- FROM
--     firmware_dev.agg_command_scheduler_stats
-- WHERE
--     grouping_id = 193
--     AND total_derate_instances > 0

-- COMMAND ----------

-- SELECT
--     SUM(total_derate_instances) AS total_derate_instances,
--     SUM(total_derate_offset_ms) AS total_derate_offset_ms,
--     date
-- FROM
--     firmware_dev.weekly_command_scheduler_stats
-- GROUP BY
--     date

-- COMMAND ----------

-- describe firmware_dev.normalized_command_scheduler_diagnostics
