-- Databricks notebook source
SELECT assert_true(getArgument("start_date") >= '2016-01-01' OR getArgument("start_date") = '') -- bad data for dates earlier than 2016-01-01

-- COMMAND ----------

create or replace temporary view cm_overheated_state as (
  select
  cm_linked_vgs.org_id,
  cm_linked_vgs.linked_cm_id as cm_device_id,
  osdthermallimiterstate.time as timestamp,
  case
    when osdthermallimiterstate.value.int_value = 1 or osdthermallimiterstate.value.int_value = 2 then 1
    else 0
  end as is_overheated,
  osdthermallimiterstate.date
from dataprep_safety.cm_linked_vgs as cm_linked_vgs
  join kinesisstats.osdthermallimiterstate as osdthermallimiterstate on
    cm_linked_vgs.org_id = osdthermallimiterstate.org_id and
    cm_linked_vgs.linked_cm_id = osdthermallimiterstate.object_id
where osdthermallimiterstate.date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),8)) --ensure we get start of interval
                                      and COALESCE(NULLIF(getArgument("end_date"), ''),  DATE_SUB(CURRENT_DATE(), 1))
);

-- COMMAND ----------

-- In order to build our transitions, we need to construct the start and end time.
-- Grab prior state and prior time for each row.
create or replace temporary view cm_overheated_lag as (
select
  cm_device_id,
  org_id,
  COALESCE(lag(timestamp) over (partition by org_id, cm_device_id order by timestamp), 0) as prev_time,
  COALESCE(lag(is_overheated) over (partition by org_id, cm_device_id order by timestamp), 0) as prev_state,
  timestamp as cur_time,
  is_overheated as cur_state
from cm_overheated_state
);

-- Look ahead to grab the next state and time.
create or replace temporary view cm_overheated_lead as (
select
  cm_device_id,
  org_id,
  timestamp as prev_time,
  is_overheated as prev_state,
  COALESCE(lead(timestamp) over (partition by org_id, cm_device_id order by timestamp), 0) as cur_time,
  COALESCE(lead(is_overheated) over (partition by org_id, cm_device_id order by timestamp), 0) as cur_state
from cm_overheated_state
);

--Create table that is a union of the transitions between each state
create or replace temporary view overheated_hist as (
    (select *
     from cm_overheated_lag lag
     where lag.prev_state != lag.cur_state
    )
    union
    (select *
     from cm_overheated_lead lead
     where lead.prev_state != lead.cur_state
    )
);

-- COMMAND ----------

-- Since we only grabbed the transitions, we don't see
-- the timestamps for consecutive reported objectStat values. To
-- create accurate intervals, we need to grab the next rows cur_time
-- since that timestamp is the end of the consecutive values (i.e. state transition).
create or replace temporary view overheated_states as (
  select
  cm_device_id,
  org_id,
  cur_time as start_ms,
  cur_state as is_overheated,
  lead(cur_time) over (partition by org_id, cm_device_id order by cur_time) as end_ms
from overheated_hist
);

--Grab intervals where the device was overheated
create or replace temporary view cm_overheated_intervals as (
  select org_id,
  cm_device_id,
  is_overheated,
  start_ms,
  COALESCE(end_ms, UNIX_TIMESTAMP(COALESCE(NULLIF(getArgument("end_date"), ''),  date_sub(CURRENT_DATE(),1)),'yyyy-MM-dd')*1000) as end_ms
  from overheated_states
  where start_ms != 0
)


-- COMMAND ----------

create or replace temporary view cm_overheating_durations as (
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
      least(coalesce(cm_overheated_intervals.end_ms, cm_vg_intervals.start_ms), cm_vg_intervals.end_ms) - greatest(cm_overheated_intervals.start_ms, cm_vg_intervals.start_ms)
    )
    as overheated_duration_ms,
  count(cm_overheated_intervals.start_ms) as overheated_count
  from (SELECT * FROM dataprep_safety.cm_vg_intervals
        WHERE date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3))
                       and COALESCE(NULLIF(getArgument("end_date"), ''),  DATE_SUB(CURRENT_DATE(), 1))) cm_vg_intervals
  left join cm_overheated_intervals as cm_overheated_intervals on
      cm_vg_intervals.org_id = cm_overheated_intervals.org_id and
    cm_vg_intervals.cm_device_id = cm_overheated_intervals.cm_device_id and
    not (cm_overheated_intervals.start_ms >= cm_vg_intervals.end_ms or cm_overheated_intervals.end_ms <= cm_vg_intervals.start_ms) and
    cm_overheated_intervals.is_overheated = 1 and
    cm_overheated_intervals.end_ms is not null and
    cm_overheated_intervals.start_ms is not null
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

create table
if not exists dataprep_safety.cm_overheating_durations
using delta
partitioned by (date)
as
select *
from cm_overheating_durations

-- COMMAND ----------

create or replace temporary view cm_overheating_durations_updates as
(
  select *
  from cm_overheating_durations
  where date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3))
                 and COALESCE(NULLIF(getArgument("end_date"), ''),  DATE_SUB(CURRENT_DATE(), 1))
)

-- COMMAND ----------

merge into dataprep_safety.cm_overheating_durations as target
using cm_overheating_durations_updates as updates
on target.org_id = updates.org_id
  and target.device_id = updates.device_id
  and target.cm_device_id = updates.cm_device_id
  and target.trip_start_ms = updates.trip_start_ms
  and target.trip_end_ms = updates.trip_end_ms
when matched then update set *
when not matched then insert * ;
