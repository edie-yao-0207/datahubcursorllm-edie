-- Databricks notebook source
create or replace temp view devices as (
  select
    date,
    org_id,
    device_id
  from data_analytics.vg3x_daily_summary
  where date >= date_sub(current_date(),10) --added in datestop for scheduled notebook
)

-- COMMAND ----------

-- Grab data from kinesisstats.location. Currently VG3x's only report one gps_fix_timestamp_utc_ms value for it's first gps
-- fix for a given location. For subsequent pings in that same location, it does not record additional values. For now, we can explicity filter out
-- those null values. Additionally, the objectstat was rolled out in the later half of 2020.
create or replace temporary view gps_fix_times as (
  select date,
    org_id,
    device_id,
    time,
    value.has_fix,
    value.latitude,
    value.longitude,
    value.gps_fix_timestamp_utc_ms,
    value.received_at_ms,
    value.received_at_ms - value.gps_fix_timestamp_utc_ms as time_fix_to_server_ms,
    value.received_at_ms - time as time_log_to_server_ms,
    time - value.gps_fix_timestamp_utc_ms as time_fix_to_log  
  from kinesisstats.location 
  where value.gps_fix_timestamp_utc_ms is not null
  and date >= date_sub(current_date(),10) --added in datestop for scheduled notebook
  and value.gps_fix_timestamp_utc_ms > 1388577600 --filtering out any data points with a timestamp < '2014-01-01'
);

-- COMMAND ----------

create or replace temporary view vg3x_gps_fix_delay as (
  select
    dev.date,
    dev.org_id,
    dev.device_id,
    SUM(CASE WHEN time_fix_to_server_ms > 60000 THEN 1 ELSE 0 END) / COUNT(*) AS perc_late_pings, -- want to idenifty devices with a high percentage of pings > 1 min delayed
    percentile_approx(gps.time_fix_to_server_ms, 0.50) AS median_time_fix_to_server_ms,
    percentile_approx(gps.time_fix_to_server_ms, 0.95) AS 95th_perc_time_fix_to_server_ms
  from devices as dev
  join gps_fix_times as gps on
    dev.date = gps.date
    and dev.org_id = gps.org_id
    and dev.device_id = gps.device_id
  group by
    dev.date,
    dev.org_id,
    dev.device_id
);

-- COMMAND ----------

create table if not exists data_analytics.vg3x_gps_fix_times
using delta
partitioned by (date)
select * from vg3x_gps_fix_delay

-- COMMAND ----------

create or replace temporary view vg3x_gps_fix_delay_updates as (
  select *
  from vg3x_gps_fix_delay
  where date >= date_sub(current_date(),10)
);


merge into data_analytics.vg3x_gps_fix_times as target
using vg3x_gps_fix_delay_updates as updates on
target.date = updates.date
and target.org_id = updates.org_id
and target.device_id = updates.device_id
when matched then update set *
when not matched then insert *
