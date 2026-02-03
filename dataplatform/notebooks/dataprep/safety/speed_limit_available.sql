-- Databricks notebook source
SELECT assert_true(getArgument("start_date") >= '2016-01-01' OR getArgument("start_date") = '') -- bad data for dates earlier than 2016-01-01

-- COMMAND ----------

create or replace temporary view cm_speed_limit_available as (
  select
    cm_linked_vgs.org_id,
    cm_linked_vgs.linked_cm_id as cm_device_id,
    osdedgespeedlimit.time as timestamp,
    case
      when osdedgespeedlimit.value.proto_value.edge_speed_limit.has_edge_speed_limit = true then 1
      else 0
    end as speed_limit_available,
    osdedgespeedlimit.date
  from dataprep_safety.cm_linked_vgs as cm_linked_vgs
  join kinesisstats.osdedgespeedlimitv2 as osdedgespeedlimit on
    cm_linked_vgs.org_id = osdedgespeedlimit.org_id and
    cm_linked_vgs.linked_cm_id = osdedgespeedlimit.object_id
  where osdedgespeedlimit.date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),8)) --ensure we get start of interval
                                   and COALESCE(NULLIF(getArgument("end_date"), ''),  DATE_SUB(CURRENT_DATE(), 1))
);

-- COMMAND ----------

-- In order to build our transitions, we need to construct the start and end time.
-- Grab prior state and prior time for each row.
create or replace temporary view cm_speedlimit_lag as (
  select
    cm_device_id,
    org_id,
    COALESCE(lag(timestamp) over (partition by org_id, cm_device_id order by timestamp), 0) as prev_time,
    COALESCE(lag(speed_limit_available) over (partition by org_id, cm_device_id order by timestamp), 0) as prev_state,
    timestamp as cur_time,
    speed_limit_available as cur_state
  from cm_speed_limit_available
);

-- Look ahead to grab the next state and time.
create or replace temporary view cm_speedlimit_lead as (
  select
    cm_device_id,
    org_id,
    timestamp as prev_time,
    speed_limit_available as prev_state,
    COALESCE(lead(timestamp) over (partition by org_id, cm_device_id order by timestamp), 0) as cur_time,
    COALESCE(lead(speed_limit_available) over (partition by org_id, cm_device_id order by timestamp), 0) as cur_state
  from cm_speed_limit_available
);

--Create table that is a union of the transitions between each state
create or replace temporary view speed_limit_hist as (
    (select *
    from cm_speedlimit_lag lag
    where lag.prev_state != lag.cur_state
    )
    union
    (select *
    from cm_speedlimit_lead lead
    where lead.prev_state != lead.cur_state
    )
);

-- COMMAND ----------

-- Since we only grabbed the transitions, we don't see
-- the timestamps for consecutive reported objectStat values. To
-- create accurate intervals, we need to grab the next rows cur_time
-- since that timestamp is the end of the consecutive values (i.e. state transition).
create or replace temporary view speed_limit_states as (
  select
    cm_device_id,
    org_id,
    cur_time as start_ms,
    cur_state as speed_limit_available,
    lead(cur_time) over (partition by org_id, cm_device_id order by cur_time) as end_ms
  from speed_limit_hist
);


create or replace temporary view cm_speed_limit_available_intervals as (
  select
    org_id,
    cm_device_id,
    speed_limit_available,
    start_ms,
    COALESCE(end_ms, UNIX_TIMESTAMP(COALESCE(NULLIF(getArgument("end_date"), ''),  date_sub(CURRENT_DATE(),1)),'yyyy-MM-dd')*1000) as end_ms
  from speed_limit_states
  where start_ms != 0
)

-- COMMAND ----------

create or replace temporary view cm_speed_limit_available_durations as (
  select
    cm_vg_intervals.date,
    cm_vg_intervals.org_id,
    cm_vg_intervals.device_id,
    cm_vg_intervals.cm_device_id,
    cm_vg_intervals.product_id,
    cm_vg_intervals.cm_product_id,
    cm_vg_intervals.start_ms AS trip_start_ms,
    cm_vg_intervals.duration_ms AS trip_duration_ms,
    cm_vg_intervals.end_ms AS trip_end_ms,
    sum
    (
      least(coalesce(cm_speed_limit_available_intervals.end_ms, cm_vg_intervals.start_ms), cm_vg_intervals.end_ms) - greatest(cm_speed_limit_available_intervals.start_ms, cm_vg_intervals.start_ms)
    )
    as speed_limit_available_duration_ms
    from (SELECT * FROM dataprep_safety.cm_vg_intervals
          WHERE date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3))
                         and COALESCE(NULLIF(getArgument("end_date"), ''),  DATE_SUB(CURRENT_DATE(), 1))) cm_vg_intervals
    left join cm_speed_limit_available_intervals cm_speed_limit_available_intervals on
      cm_vg_intervals.org_id = cm_speed_limit_available_intervals.org_id and
      cm_vg_intervals.cm_device_id = cm_speed_limit_available_intervals.cm_device_id and
      not (cm_speed_limit_available_intervals.start_ms >= cm_vg_intervals.end_ms or cm_speed_limit_available_intervals.end_ms <= cm_vg_intervals.start_ms) and
      cm_speed_limit_available_intervals.speed_limit_available = 1 and
      cm_speed_limit_available_intervals.end_ms is not null and
      cm_speed_limit_available_intervals.start_ms is not null
    group by
      cm_vg_intervals.org_id,
      cm_vg_intervals.device_id,
      cm_vg_intervals.cm_device_id,
      cm_vg_intervals.product_id,
      cm_vg_intervals.cm_product_id,
      cm_vg_intervals.start_ms,
      cm_vg_intervals.duration_ms,
      cm_vg_intervals.end_ms,
      cm_vg_intervals.date
);

-- COMMAND ----------


create table if not exists dataprep_safety.cm_speed_limit_available_durations
using delta
partitioned by (date)
as
select * from cm_speed_limit_available_durations

-- COMMAND ----------

create or replace temporary view cm_speed_limit_available_durations_updates as (
  select * from cm_speed_limit_available_durations where date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3))
                                                                  and COALESCE(NULLIF(getArgument("end_date"), ''),  DATE_SUB(CURRENT_DATE(), 1))
)

-- COMMAND ----------

merge into dataprep_safety.cm_speed_limit_available_durations as target
using cm_speed_limit_available_durations_updates as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.cm_device_id = updates.cm_device_id
and target.trip_start_ms = updates.trip_start_ms
and target.trip_end_ms = updates.trip_end_ms
when matched then update set *
when not matched then insert * ;
