-- Databricks notebook source

SELECT assert_true(getArgument("start_date") >= '2016-01-01' OR getArgument("start_date") = '') -- bad data for dates earlier than 2016-01-01

-- COMMAND ----------

create or replace temporary view cm_safe_mode as (
  select a.org_id,
  a.linked_cm_id as cm_device_id,
  b.time as timestamp,
  case
    when b.value.int_value = 3 then 1
    else 0
  end as is_safe_mode,
  b.date
  from dataprep_safety.cm_linked_vgs as a
  join kinesisstats.osdthermallimiterstate as b on
    a.org_id = b.org_id and
    a.linked_cm_id = b.object_id
  where b.date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  DATE_SUB(CURRENT_DATE(),8)) 
                   and COALESCE(NULLIF(getArgument("end_date"), ''), DATE_SUB(CURRENT_DATE(), 1))
);

-- COMMAND ----------

create or replace temporary view cm_safemode_lag as (
select
  cm_device_id,
  org_id,
  COALESCE(lag(timestamp) over (partition by org_id, cm_device_id order by timestamp), 0) as prev_time,
  COALESCE(lag(is_safe_mode) over (partition by org_id, cm_device_id order by timestamp), 0) as prev_state,
  timestamp as cur_time,
  is_safe_mode as cur_state
from cm_safe_mode
);

create or replace temporary view cm_safemode_lead as (
select
  cm_device_id,
  org_id,
  timestamp as prev_time,
  is_safe_mode as prev_state,
  COALESCE(lead(timestamp) over (partition by org_id, cm_device_id order by timestamp), 0) as cur_time,
  COALESCE(lead(is_safe_mode) over (partition by org_id, cm_device_id order by timestamp), 0) as cur_state
from cm_safe_mode
);

create or replace temporary view safemode_hist as (
  (select *
   from cm_safemode_lag lag
   where lag.prev_state != lag.cur_state
  )
  union
  (select *
   from cm_safemode_lead lead
   where lead.prev_state != lead.cur_state
  )
);

-- COMMAND ----------
create or replace temporary view safemode_states as (
  select
  cm_device_id,
  org_id,
  cur_time as start_ms,
  cur_state as is_safe_mode,
  lead(cur_time) over (partition by org_id, cm_device_id order by cur_time) as end_ms
from safemode_hist
);

create or replace temporary view cm_safe_mode_intervals as (
  select org_id,
  cm_device_id,
  is_safe_mode,
  start_ms,
  COALESCE(end_ms, UNIX_TIMESTAMP(COALESCE(NULLIF(getArgument("end_date"), ''),  date_sub(CURRENT_DATE(),1)),'yyyy-MM-dd')*1000) as end_ms
from safemode_states
where start_ms != 0
)


-- COMMAND ----------


create or replace temporary view cm_safe_mode_durations as (
  select
    a.date,
    a.org_id,
    a.device_id,
    a.cm_device_id,
    a.product_id,
    a.cm_product_id,
    a.start_ms AS trip_start_ms,
    a.duration_ms AS trip_duration_ms,
    a.end_ms AS trip_end_ms,
    sum
    (
      least(coalesce(b.end_ms, a.start_ms), a.end_ms) - greatest(b.start_ms, a.start_ms)
    )
    as safe_mode_duration_ms,
    count(b.start_ms) as safe_mode_count
    from dataprep_safety.cm_vg_intervals a
    left join cm_safe_mode_intervals b on
      a.org_id = b.org_id and
      a.cm_device_id = b.cm_device_id and
      not (b.start_ms >= a.end_ms or b.end_ms <= a.start_ms) and
      b.is_safe_mode = 1 and
      b.end_ms is not null and
      b.start_ms is not null
    WHERE a.date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3))
                     and COALESCE(NULLIF(getArgument("end_date"), ''),  DATE_SUB(CURRENT_DATE(), 1))
    group by
      a.org_id,
      a.device_id,
      a.cm_device_id,
      a.product_id,
      a.cm_product_id,
      a.start_ms,
      a.duration_ms,
      a.end_ms,
      a.date
);

-- COMMAND ----------

create table if not exists dataprep_safety.cm_safe_mode_durations
using delta
partitioned by (date)
as
select * from cm_safe_mode_durations

-- COMMAND ----------

create or replace temporary view cm_safe_mode_durations_updates as (
  select * from cm_safe_mode_durations where date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3))
                                                      and COALESCE(NULLIF(getArgument("end_date"), ''),  DATE_SUB(CURRENT_DATE(), 1))
)

-- COMMAND ----------

merge into dataprep_safety.cm_safe_mode_durations as target
using cm_safe_mode_durations_updates as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.cm_device_id = updates.cm_device_id
and target.trip_start_ms = updates.trip_start_ms
and target.trip_end_ms = updates.trip_end_ms
when matched then update set *
when not matched then insert * ;
