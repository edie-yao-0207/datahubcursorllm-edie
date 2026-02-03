-- Databricks notebook source

-- MAGIC %md
-- MAGIC ## Read object stats
-- MAGIC
-- MAGIC We keep the databreak and terminate stats and consider them as "down". Databreaks are sent upon service startup and terminates upon shutdown.
-- MAGIC
-- MAGIC ```
-- MAGIC enum HarshEventAlgorithmState {
-- MAGIC    HE_ALGORITHM_STATE_INVALID = 0;
-- MAGIC    HE_ALGORITHM_STATE_RUNNING = 1;
-- MAGIC    HE_ALGORITHM_STATE_STOPPED = 2;
-- MAGIC    HE_ALGORITHM_STATE_NO_ORIENTATION = 3;
-- MAGIC    HE_ALGORITHM_STATE_NO_GRAVITY = 4;
-- MAGIC    HE_ALGORITHM_STATE_NO_CONFIG = 5;
-- MAGIC    HE_ALGORITHM_STATE_NO_VEHICLE_TYPE = 6;
-- MAGIC    HE_ALGORITHM_STATE_FILTER_INIT = 7;
-- MAGIC    HE_ALGORITHM_STATE_DC_BIAS_INIT = 8;
-- MAGIC }
-- MAGIC ```

-- COMMAND ----------

create or replace temp view algostate_harsh_v1 as (
  select
    org_id,
    object_id as device_id,
    date,
    time,
    value.int_value as state
  from kinesisstats.osdhev1harsheventdetectionrunning as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date > current_date()-8
    and a.time <= b.time_ms
);

-- COMMAND ----------

create or replace temp view algostate_crash_v1 as (
  select
    org_id,
    object_id as device_id,
    date,
    time,
    value.int_value as state
  from kinesisstats.osdhev1crashdetectionrunning as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date > current_date()-8
    and a.time <= b.time_ms
);

-- COMMAND ----------

create or replace temp view algostate_harsh_v2 as (
  select
    org_id,
    object_id as device_id,
    date,
    time,
    value.int_value as state
  from kinesisstats.osdhev2harsheventdetectionrunning as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date > current_date()-8
    and a.time <= b.time_ms
);

-- COMMAND ----------

create or replace temp view algostate_crash_v2 as (
  select
    org_id,
    object_id as device_id,
    date,
    time,
    value.int_value as state
  from kinesisstats.osdhev2crashdetectionrunning as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date > current_date()-8
    and a.time <= b.time_ms
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## For each day, devices that are reporting uptime object stats
-- MAGIC
-- MAGIC We'll use this to ignore time buckets where we don't have them, because we don't want to mistakenly report the algorithms being down if we're just not reporting the stats.

-- COMMAND ----------

create or replace temp view device_days_reporting_uptime as (
  select
    date,
    device_id
  from
  (
  (select
    date,
    object_id as device_id
  from kinesisstats.osdhev1crashdetectionrunning as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where a.time <= b.time_ms)
  union
  (select
    date,
    object_id as device_id
  from kinesisstats.osdhev2crashdetectionrunning as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where a.time <= b.time_ms)
  union
  (select
    date,
    object_id as device_id
  from kinesisstats.osdhev2harsheventdetectionrunning as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where a.time <= b.time_ms)
  union
  (select
    date,
    object_id as device_id
  from kinesisstats.osdhev2harsheventdetectionrunning as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where a.time <= b.time_ms)
  )
  group by 1,2
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Interval tables
-- MAGIC
-- MAGIC Get tables where each row is an interval with a given state (up or down).

-- COMMAND ----------

-- harsh v1 --

create or replace temp view algostate_harsh_v1_updown as (
  select
    org_id,
    device_id,
    date,
    time,
    case
      when state != 1 then "down"
      when state == 1 then "up"
    end as state
  from algostate_harsh_v1
);

create or replace temp view algostate_harsh_v1_state_changes as (
  with lags as (
    select
      *,
      LAG(state, 1, "n/a") over (partition by device_id order by time) as previous_state
    from algostate_harsh_v1_updown
  )
  select
    org_id,
    device_id,
    date,
    time,
    state
  from lags
  where state != previous_state
);

create or replace temp view algostate_harsh_v1_intervals as (
  with lags as (
    select
      *,
      LAG(time, -1, to_unix_timestamp(current_timestamp())*1e3) over (partition by device_id order by time) as next_time
    from algostate_harsh_v1_state_changes
  )
  select
    org_id,
    device_id,
    time as start_time,
    date as start_date,
    next_time as end_time,
    next_time - time as duration_ms,
    state
  from lags
);

-- COMMAND ----------

-- crash v1 --

create or replace temp view algostate_crash_v1_updown as (
  select
    org_id,
    device_id,
    date,
    time,
    case
      when state != 1 then "down"
      when state == 1 then "up"
    end as state
  from algostate_crash_v1
);

create or replace temp view algostate_crash_v1_state_changes as (
  with lags as (
    select
      *,
      LAG(state, 1, "n/a") over (partition by device_id order by time) as previous_state
    from algostate_crash_v1_updown
  )
  select
    org_id,
    device_id,
    date,
    time,
    state
  from lags
  where state != previous_state
);

create or replace temp view algostate_crash_v1_intervals as (
  with lags as (
    select
      *,
      LAG(time, -1, to_unix_timestamp(current_timestamp())*1e3) over (partition by device_id order by time) as next_time
    from algostate_crash_v1_state_changes
  )
  select
    org_id,
    device_id,
    time as start_time,
    date as start_date,
    next_time as end_time,
    next_time - time as duration_ms,
    state
  from lags
);

-- COMMAND ----------

-- harsh v2 --

create or replace temp view algostate_harsh_v2_updown as (
  select
    org_id,
    device_id,
    date,
    time,
    case
      when state != 1 then "down"
      when state == 1 then "up"
    end as state
  from algostate_harsh_v2
);

create or replace temp view algostate_harsh_v2_state_changes as (
  with lags as (
    select
      *,
      LAG(state, 1, "n/a") over (partition by device_id order by time) as previous_state
    from algostate_harsh_v2_updown
  )
  select
    org_id,
    device_id,
    date,
    time,
    state
  from lags
  where state != previous_state
);

create or replace temp view algostate_harsh_v2_intervals as (
  with lags as (
    select
      *,
      LAG(time, -1, to_unix_timestamp(current_timestamp())*1e3) over (partition by device_id order by time) as next_time
    from algostate_harsh_v2_state_changes
  )
  select
    org_id,
    device_id,
    time as start_time,
    date as start_date,
    next_time as end_time,
    next_time - time as duration_ms,
    state
  from lags
);

-- COMMAND ----------

-- crash v2 --

create or replace temp view algostate_crash_v2_updown as (
  select
    org_id,
    device_id,
    date,
    time,
    case
      when state != 1 then "down"
      when state == 1 then "up"
    end as state
  from algostate_crash_v2
);

create or replace temp view algostate_crash_v2_state_changes as (
  with lags as (
    select
      *,
      LAG(state, 1, "n/a") over (partition by device_id order by time) as previous_state
    from algostate_crash_v2_updown
  )
  select
    org_id,
    device_id,
    date,
    time,
    state
  from lags
  where state != previous_state
);

create or replace temp view algostate_crash_v2_intervals as (
  with lags as (
    select
      *,
      LAG(time, -1, to_unix_timestamp(current_timestamp())*1e3) over (partition by device_id order by time) as next_time
    from algostate_crash_v2_state_changes
  )
  select
    org_id,
    device_id,
    time as start_time,
    date as start_date,
    next_time as end_time,
    next_time - time as duration_ms,
    state
  from lags
);

-- Fix reporting bug where hev2 crash reports status 4 (waiting for gravity) where it's actually running (should be 1) - happens
-- when hev2 crash starts with an orientation (at boot). Simply join with hev2 harsh intervals, and consider that if hev2 harsh
-- is running, we have orientation so hev2 crash is necessarily running.
-- If harsh v2 is not running but we have gravity, then there's no reporting issue because the bug does not trigger in that case.
-- It only triggers when hev2 starts with an orientation.
create or replace temp view algostate_crash_v2_intervals_fixed as (
  with crash_harsh_combined as (
    select
      c.org_id,
      c.device_id,
      greatest(c.start_date, h.start_date) as start_date,
      greatest(c.start_time, h.start_time) as start_time,
      least(c.end_time, h.end_time) as end_time,
      c.state as crash_state,
      h.state as harsh_state
    from algostate_crash_v2_intervals as c
    left join algostate_harsh_v2_intervals as h
      on c.device_id = h.device_id
        and (h.start_time between c.start_time and c.end_time or c.start_time between h.start_time and h.end_time)
  )
  select
    org_id,
    device_id,
    start_date,
    start_time,
    end_time,
    end_time - start_time as duration_ms,
    case
      when crash_state = "up" or harsh_state = "up" then "up" else "down"
    end as state
    from crash_harsh_combined
);

-- COMMAND ----------

create or replace temp view crash_intervals_combined as (
  select
    v1.org_id,
    v1.device_id,
    greatest(v1.start_time, v2.start_time) as start_time,
    greatest(v1.start_date, v2.start_date) as start_date,
    least(v1.end_time, v2.end_time) as end_time,
    case
      when v1.state == "up" or v2.state == "up" then "up"
      else "down"
    end as state,
    coalesce(v1.state, "n/a") as state_v1,
    coalesce(v2.state, "n/a") as state_v2
  from
    algostate_crash_v1_intervals as v1
  left join
    algostate_crash_v2_intervals_fixed as v2
    on
      v1.device_id = v2.device_id
      and (v1.start_time between v2.start_time and v2.end_time or v2.start_time between v1.start_time and v1.end_time)
);

create or replace temp view crash_intervals_final as (
  select
    org_id,
    device_id,
    start_time,
    start_date as start_date_approx,
    from_unixtime(start_time/1e3) as start_date,
    end_time,
    from_unixtime(end_time/1e3) as end_date,
    (end_time - start_time) / 1e3 as duration_secs,
    state,
    state_v1,
    state_v2
  from crash_intervals_combined
);

-- COMMAND ----------

create or replace temp view harsh_intervals_combined as (
  select
    v1.org_id,
    v1.device_id,
    greatest(v1.start_time, v2.start_time) as start_time,
    greatest(v1.start_date, v2.start_date) as start_date,
    least(v1.end_time, v2.end_time) as end_time,
    case
      when v1.state == "up" or v2.state == "up" then "up"
      else "down"
    end as state,
    coalesce(v1.state, "n/a") as state_v1,
    coalesce(v2.state, "n/a") as state_v2
  from
    algostate_harsh_v1_intervals as v1
  left join
    algostate_harsh_v2_intervals as v2
    on
      v1.device_id = v2.device_id
      and (v1.start_time between v2.start_time and v2.end_time or v2.start_time between v1.start_time and v1.end_time)
);

create or replace temp view harsh_intervals_final as (
  select
    org_id,
    device_id,
    start_time,
    start_date as start_date_approx,
    from_unixtime(start_time/1e3) as start_date,
    end_time,
    from_unixtime(end_time/1e3) as end_date,
    (end_time - start_time) / 1e3 as duration_secs,
    state,
    state_v1,
    state_v2
  from harsh_intervals_combined
);

-- COMMAND ----------

-- TODO: split overlapping trips?
create or replace temp view trips_cleaned as (
  select
    date,
    org_id,
    device_id,
    start_ms,
    max(end_ms) as end_ms -- if there's multiple trips with same start_ms, keep the longest one
  from
    trips2db_shards.trips
  where
    date > date_sub(current_date(), 10)
    and version = 101
  group by 1,2,3,4
);

-- COMMAND ----------

-- CRASH - join with trips, explode intervals
create or replace temp view trips_crash_uptime_intervals as (
  select
    t.org_id,
    t.device_id,
    t.start_ms as trip_start_ms,
    greatest(t.start_ms, c.start_time) as start_ms,
    least(t.end_ms, c.end_time) as end_ms,
    coalesce(c.state, "n/a") as state,
    coalesce(c.state_v1, "n/a") as state_v1,
    coalesce(c.state_v2, "n/a") as state_v2
  from
    trips_cleaned as t
  left join
    crash_intervals_final as c
    on
      t.device_id = c.device_id
      and (t.start_ms between c.start_time and c.end_time or c.start_time between t.start_ms and t.end_ms)
);

-- compute interval duration and clean fields up
create or replace temp view trips_crash_uptime_intervals_clean as (
  select
    org_id,
    device_id,
    date(from_unixtime(start_ms/1e3)) as start_date,
    start_ms,
    end_ms,
    (end_ms - start_ms) / 1e3 as duration_secs,
    state,
    state_v1,
    state_v2
  from trips_crash_uptime_intervals
);

-- add org type
create or replace temp view trips_crash_uptime_intervals_final as (
  select
    intervals.*,
    case when coalesce(o.internal_type, 0) != 0 then "internal" else "customer" end as org_type
  from
    trips_crash_uptime_intervals_clean as intervals
  left join
    clouddb.organizations as o
    on intervals.org_id = o.id
);

-- Filter out intervals where most likely the device was not reporting uptime (old firmware).
-- Require that some uptime object stats have been received on the day of the interval, and also
-- the day before and the day after, to be extra safe.
-- it's very unlikely for this query to unexpectedly filter out intervals, because we report the uptime
-- object stats on change + every five minutes.
create or replace temp view trips_crash_uptime_for_reporting_devices as (
  select
    up.*
  from trips_crash_uptime_intervals_final as up
  inner join device_days_reporting_uptime as dd
  on
    up.device_id = dd.device_id
    and up.start_date = dd.date
  inner join device_days_reporting_uptime as dd_previous
  on
    up.device_id = dd_previous.device_id
    and up.start_date = date_sub(dd_previous.date, 1)
  inner join device_days_reporting_uptime as dd_next
  on
    up.device_id = dd_next.device_id
    and up.start_date = date_add(dd_next.date, 1)
);

-- There are duplicate intervals because of overlapping trips. Here we remove the duplicates, that
-- are annoying because 1) it makes it harder to merge into the dataprep table and 2) it double
-- counts those intervals in terms of uptime calculation.
-- The trips_cleaned view should be updated to get rid of overlapping trips.
create or replace temp view trips_crash_uptime_for_reporting_devices_deduped as (
  select
    org_id,
    device_id,
    start_date,
    start_ms,
    end_ms,
    duration_secs,
    state,
    state_v1,
    state_v2,
    org_type
  from
    trips_crash_uptime_for_reporting_devices
  group by 1,2,3,4,5,6,7,8,9,10
);

-- COMMAND ----------

-- HARSH - join with trips, explode intervals
create or replace temp view trips_harsh_uptime_intervals as (
  select
    t.org_id,
    t.device_id,
    t.start_ms as trip_start_ms,
    greatest(t.start_ms, c.start_time) as start_ms,
    least(t.end_ms, c.end_time) as end_ms,
    coalesce(c.state, "n/a") as state,
    coalesce(c.state_v1, "n/a") as state_v1,
    coalesce(c.state_v2, "n/a") as state_v2
  from
    trips_cleaned as t
  left join
    harsh_intervals_final as c
    on
      t.device_id = c.device_id
      and (t.start_ms between c.start_time and c.end_time or c.start_time between t.start_ms and t.end_ms)
);

-- compute interval duration and clean fields up
create or replace temp view trips_harsh_uptime_intervals_clean as (
  select
    org_id,
    device_id,
    date(from_unixtime(start_ms/1e3)) as start_date,
    start_ms,
    end_ms,
    (end_ms - start_ms) / 1e3 as duration_secs,
    state,
    state_v1,
    state_v2
  from trips_harsh_uptime_intervals
);

-- add org type
create or replace temp view trips_harsh_uptime_intervals_final as (
  select
    intervals.*,
    case when coalesce(o.internal_type, 0) != 0 then "internal" else "customer" end as org_type
  from
    trips_harsh_uptime_intervals_clean as intervals
  left join
    clouddb.organizations as o
    on intervals.org_id = o.id
);

-- Filter out intervals where most likely the device was not reporting uptime (old firmware).
-- Require that some uptime object stats have been received on the day of the interval, and also
-- the day before and the day after, to be extra safe.
-- it's very unlikely for this query to unexpectedly filter out intervals, because we report the uptime
-- object stats on change + every five minutes.
create or replace temp view trips_harsh_uptime_for_reporting_devices as (
  select
    up.*
  from trips_harsh_uptime_intervals_final as up
  inner join device_days_reporting_uptime as dd
  on
    up.device_id = dd.device_id
    and up.start_date = dd.date
  inner join device_days_reporting_uptime as dd_previous
  on
    up.device_id = dd_previous.device_id
    and up.start_date = date_sub(dd_previous.date, 1)
  inner join device_days_reporting_uptime as dd_next
  on
    up.device_id = dd_next.device_id
    and up.start_date = date_add(dd_next.date, 1)
);

-- There are duplicate intervals because of overlapping trips. Here we remove the duplicates, that
-- are annoying because 1) it makes it harder to merge into the dataprep table and 2) it double
-- counts those intervals in terms of uptime calculation.
-- The trips_cleaned view should be updated to get rid of overlapping trips.
create or replace temp view trips_harsh_uptime_for_reporting_devices_deduped as (
  select
    org_id,
    device_id,
    start_date,
    start_ms,
    end_ms,
    duration_secs,
    state,
    state_v1,
    state_v2,
    org_type
  from
    trips_harsh_uptime_for_reporting_devices
  group by 1,2,3,4,5,6,7,8,9,10
);

-- COMMAND ----------

create or replace temp view harsh_v2_unaligned as (
  select
    org_id,
    device_id,
    date,
    time,
    case
      when state == 3 then "waiting_alignment"
      else "other"
    end as state
  from algostate_harsh_v2
);

create or replace temp view harsh_v2_unaligned_state_changes as (
  with lags as (
    select
      *,
      LAG(state, 1) over (partition by device_id order by time) as previous_state
    from harsh_v2_unaligned
  )
  select
    org_id,
    device_id,
    date,
    time,
    state
  from lags
  where state != previous_state
);

create or replace temp view harsh_v2_unaligned_intervals as (
  with lags as (
    select
      *,
      LAG(time, -1, to_unix_timestamp(current_timestamp())*1e3) over (partition by device_id order by time) as to_time,
      LAG(date, -1, string(date(current_date()))) over (partition by device_id order by time) as to_date
    from harsh_v2_unaligned_state_changes
  )
  select
    org_id,
    device_id,
    time as from_time,
    date as from_date,
    to_time,
    to_date,
    state
  from lags
);

-- COMMAND ----------

-- join with trips, explode intervals
create or replace temp view harsh_v2_unaligned_trips as (
  select
    t.org_id,
    t.device_id,
    t.start_ms as trip_start_ms,
    greatest(t.start_ms, c.from_time) as start_ms,
    least(t.end_ms, c.to_time) as end_ms,
    coalesce(c.state, "other") as state
  from
    trips_cleaned as t
  left join
    harsh_v2_unaligned_intervals as c
    on
      t.device_id = c.device_id
      and (t.start_ms between c.from_time and c.to_time or c.from_time between t.start_ms and t.end_ms)
);

-- compute interval duration and clean fields up
create or replace temp view harsh_v2_unaligned_trips_clean as (
  select
    org_id,
    device_id,
    date(from_unixtime(start_ms/1e3)) as start_date,
    start_ms,
    date(from_unixtime(end_ms/1e3)) as end_date,
    end_ms,
    (end_ms - start_ms) / 1e3 as duration_secs,
    state
  from harsh_v2_unaligned_trips
);

-- add org type
create or replace temp view harsh_v2_unaligned_trips_clean_final as (
  select
    intervals.*,
    case when coalesce(o.internal_type, 0) != 0 then "internal" else "customer" end as org_type
  from
    harsh_v2_unaligned_trips_clean as intervals
  left join
    clouddb.organizations as o
    on intervals.org_id = o.id
);

-- Filter out intervals where most likely the device was not reporting uptime (old firmware).
-- Require that some uptime object stats have been received on the day of the interval, and also
-- the day before and the day after, to be extra safe.
-- it's very unlikely for this query to unexpectedly filter out intervals, because we report the uptime
-- object stats on change + every five minutes.
create or replace temp view device_days_reporting_hev2_status as (
  select
    date,
    object_id as device_id
  from kinesisstats.osdhev2harsheventdetectionrunning as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where a.time <= b.time_ms
  group by 1,2
);

create or replace temp view harsh_v2_unaligned_trips_clean_final_for_reporting_devices as (
  select
    up.*
  from harsh_v2_unaligned_trips_clean_final as up
  inner join device_days_reporting_hev2_status as dd
  on
    up.device_id = dd.device_id
    and up.start_date = dd.date
  inner join device_days_reporting_hev2_status as dd_previous
  on
    up.device_id = dd_previous.device_id
    and up.start_date = date_sub(dd_previous.date, 1)
  inner join device_days_reporting_hev2_status as dd_next
  on
    up.device_id = dd_next.device_id
    and up.start_date = date_add(dd_next.date, 1)
);

-- There are duplicate intervals because of overlapping trips. Here we remove the duplicates, that
-- are annoying because 1) it makes it harder to merge into the dataprep table and 2) it double
-- counts those intervals in terms of uptime calculation.
-- The trips_cleaned view should be updated to get rid of overlapping trips.
create or replace temp view harsh_v2_unaligned_trips_clean_final_for_reporting_devices_deduped as (
  select
    org_id,
    device_id,
    start_date,
    start_ms,
    end_date,
    end_ms,
    duration_secs,
    state,
    org_type
  from
    harsh_v2_unaligned_trips_clean_final_for_reporting_devices
  group by 1,2,3,4,5,6,7,8,9
);

-- COMMAND ----------

-- daily job queries last 7 days in case there's ingestion delays (e.g. gateway offline or out of coverage)
MERGE INTO dataprep_safety.he_uptime_device_config_hev2_enabled AS target
USING (

with temp as (
  select
    date,
    time,
    object_id as device_id,
    org_id,
    case
      when coalesce(s3_proto_value.reported_device_config.device_config.accel_mgr_config.enable_harsh_event_v2, 1) == 1 then true else false -- enabled by default
    end as hev2_enabled
  from
    -- querying s3bigstats.osdreporteddeviceconfig_raw is preferred over kinesisstats.osdreporteddeviceconfig_with_s3_big_stat for better performance, but we must de-dedup entries
    s3bigstats.osdreporteddeviceconfig_raw
  where
    date > current_date() - 7
    and org_id != 0
    and org_id != 1
)
select
  date,
  time,
  device_id,
  org_id,
  max(hev2_enabled) AS hev2_enabled
from
  temp
group by
  1, 2, 3, 4

) AS updates ON
target.date = updates.date
AND target.time = updates.time
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- If harsh event detection is turned off by the customer (sensitivity set to off in org or device settings), enable_harsh_event_v2 is set to 2 (disabled)
-- by config builder. And HEv2 will be disabled in firmware (won't run at all).
-- HEv1 in firmware is also self-disabling by seeing that there is no AccelHarshDetectConfig or AccelCrashDetectConfig in the device config.
create or replace temp view hev2_enabled_intervals as (
  with lags as (
    select
      *,
      LAG(hev2_enabled, 1) over (partition by device_id order by time) as prev_hev2_enabled
    from dataprep_safety.he_uptime_device_config_hev2_enabled where date > current_date() - 180
  ),
  state_changes as (
    select
      org_id,
      device_id,
      date,
      time,
      hev2_enabled
    from lags
    where prev_hev2_enabled != hev2_enabled or prev_hev2_enabled is null
  )
  select
    *,
    LAG(time, -1, to_unix_timestamp(current_timestamp())*1e3) over (partition by device_id order by time) as next_time
  from state_changes
);

-- COMMAND ----------

MERGE INTO dataprep_safety.he_uptime_crash_intervals AS target
USING (select * from trips_crash_uptime_for_reporting_devices_deduped where start_date > current_date()-8) AS updates ON
target.start_date = updates.start_date
AND target.start_ms = updates.start_ms
AND target.end_ms = updates.end_ms
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- crash
create or replace temp view trips_crash_uptime_for_reporting_devices_deduped_with_config as (
  with cleaned_data as (
    select
     crash.org_id,
     crash.device_id,
     crash.state,
     crash.state_v1,
     crash.state_v2,
     org_type,
     greatest(crash.start_ms, config.time) as start_ms,
     least(crash.end_ms, config.next_time) as end_ms,
     config.hev2_enabled
   from dataprep_safety.he_uptime_crash_intervals as crash
   left join hev2_enabled_intervals as config
     on crash.device_id = config.device_id
       and (crash.start_ms between config.time and config.next_time or config.time between crash.start_ms and crash.end_ms)
    -- discard trips where hev2 is disabled, regardless of hev1. hev2 is disabled only for phase 2 orgs, CMx2 devices and Modi orgs, otherwise it runs dark-launched.
    -- this is a few devices, and we're ok discarding them from uptime metrics (instead of tracking hev1 uptime only)
   where config.hev2_enabled = true
 )
 select
   org_id,
   device_id,
   date(from_unixtime(start_ms/1e3)) as start_date,
   start_ms,
   end_ms,
   (end_ms - start_ms) / 1e3 as duration_secs,
   state,
   state_v1,
   state_v2,
   org_type,
   hev2_enabled
 from cleaned_data
);

-- COMMAND ----------

drop table if exists dataprep_safety.he_uptime_crash_intervals_cleaned;
create table if not exists dataprep_safety.he_uptime_crash_intervals_cleaned using delta partitioned by (start_date) as (
  select * from trips_crash_uptime_for_reporting_devices_deduped_with_config
);

MERGE INTO dataprep_safety.he_uptime_harsh_intervals AS target
USING (select * from trips_harsh_uptime_for_reporting_devices_deduped where start_date > current_date()-8) AS updates ON
target.start_date = updates.start_date
AND target.start_ms = updates.start_ms
AND target.end_ms = updates.end_ms
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

-- harsh
create or replace temp view trips_harsh_uptime_for_reporting_devices_deduped_with_config as (
  with cleaned_data as (
    select
     harsh.org_id,
     harsh.device_id,
     harsh.state,
     harsh.state_v1,
     harsh.state_v2,
     org_type,
     greatest(harsh.start_ms, config.time) as start_ms,
     least(harsh.end_ms, config.next_time) as end_ms
   from dataprep_safety.he_uptime_harsh_intervals as harsh
   left join hev2_enabled_intervals as config
     on harsh.device_id = config.device_id
       and (harsh.start_ms between config.time and config.next_time or config.time between harsh.start_ms and harsh.end_ms)
    -- discard trips where hev2 is disabled, regardless of hev1. hev2 is disabled only for phase 2 orgs, CMx2 devices and Modi orgs, otherwise it runs dark-launched.
    -- this is a few devices, and we're ok discarding them from uptime metrics (instead of tracking hev1 uptime only)
   where config.hev2_enabled is true
 )
 select
   org_id,
   device_id,
   date(from_unixtime(start_ms/1e3)) as start_date,
   start_ms,
   end_ms,
   (end_ms - start_ms) / 1e3 as duration_secs,
   state,
   state_v1,
   state_v2,
   org_type
 from cleaned_data
);

-- COMMAND ----------

drop table if exists dataprep_safety.he_uptime_harsh_intervals_cleaned;
create table if not exists dataprep_safety.he_uptime_harsh_intervals_cleaned using delta partitioned by (start_date) as (
  select * from trips_harsh_uptime_for_reporting_devices_deduped_with_config
);

-- COMMAND ----------

MERGE INTO dataprep_safety.he_uptime_hev2_unaligned AS target
USING (select * from harsh_v2_unaligned_trips_clean_final_for_reporting_devices_deduped where start_date > current_date()-8) AS updates ON
target.start_date = updates.start_date
AND target.end_date = updates.end_date
AND target.start_ms = updates.start_ms
AND target.end_ms = updates.end_ms
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
