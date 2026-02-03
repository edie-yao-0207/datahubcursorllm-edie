-- Databricks notebook source
create or replace temporary view cm_power_states as (
  select
    cm_linked_vgs.org_id,
    cm_linked_vgs.linked_cm_id,
    cm_linked_vgs.vg_device_id as device_id,
    osdpowerstate.time as timestamp,
    case
      when osdpowerstate.value.int_value == 1 then 1
      else 0
    end as has_power
  from dataprep_safety.cm_linked_vgs cm_linked_vgs
  left join (SELECT * FROM kinesisstats.osdpowerstate
             WHERE date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),30), ''),  date_sub(CURRENT_DATE(),35)) -- make sure we get the start of interval for 5 day period we will write to
                            and COALESCE(NULLIF(getArgument("end_date"), ''),  date_sub(current_date(), 1))) osdpowerstate
    on cm_linked_vgs.org_id = osdpowerstate.org_id
    and cm_linked_vgs.linked_cm_id = osdpowerstate.object_id
  where osdpowerstate.value.is_databreak = false and
    osdpowerstate.value.is_end = false and
    cm_linked_vgs.linked_cm_id is not null and
    osdpowerstate.date <= date_sub(current_date(), 1)
)

-- COMMAND ----------

-- In order to build our transitions, we need to construct the start and end time.
-- Grab prior state and prior time for each row.
create or replace temporary view cm_power_lag as (
  select
    linked_cm_id,
    device_id,
    org_id,
    COALESCE(lag(timestamp) over (partition by org_id, linked_cm_id, device_id order by timestamp), 0) as prev_time,
    COALESCE(lag(has_power) over (partition by org_id, linked_cm_id, device_id order by timestamp), 0) as prev_state,
    timestamp as cur_time,
    has_power as cur_state
  from cm_power_states
);

-- Look ahead to grab the next state and time.
create or replace temporary view cm_power_lead as (
  select
    linked_cm_id,
    device_id,
    org_id,
    timestamp as prev_time,
    has_power as prev_state,
    COALESCE(lead(timestamp) over (partition by org_id, linked_cm_id, device_id order by timestamp), 0) as cur_time,
    COALESCE(lead(has_power) over (partition by org_id, linked_cm_id, device_id order by timestamp), 0) as cur_state
  from cm_power_states
);

--Create table that is a union of the transitions between each state

create or replace temporary view power_hist as (
  (select *
   from cm_power_lag lag
   where lag.prev_state != lag.cur_state
  )
  union
  (select *
   from cm_power_lead lead
   where lead.prev_state != lead.cur_state
  )
);


-- COMMAND ----------

-- Since we only grabbed the transitions, we don't see
-- the timestamps for consecutive reported objectStat values. To
-- create accurate intervals, we need to grab the next rows cur_time
-- since that timestamp is the end of the consecutive values (i.e. state transition).
create or replace temporary view power_states as (
  select
    linked_cm_id,
    device_id,
    org_id,
    cur_time as start_ms,
    cur_state as has_power,
    lead(cur_time) over (partition by org_id, linked_cm_id, device_id order by cur_time) as end_ms
  from power_hist
);

-- Create final view that have both unpowered and powered ranges
create or replace temporary view cm_power_ranges as (
  select
    org_id,
    linked_cm_id,
    device_id,
    has_power,
    start_ms,
    COALESCE(end_ms, UNIX_TIMESTAMP(COALESCE(NULLIF(getArgument("end_date"), ''),  date_sub(CURRENT_DATE(),1)),'yyyy-MM-dd')*1000) as end_ms --fill open interval
  from power_states
  where start_ms != 0
)

-- COMMAND ----------

create table if not exists dataprep_safety.cm_power_ranges
using delta
as
select * from cm_power_ranges

-- COMMAND ----------

create or replace temporary view cm_power_ranges_updates as (
  select DISTINCT * from cm_power_ranges
  where start_ms between UNIX_TIMESTAMP(COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),5)),'yyyy-MM-dd')*1000
                 and UNIX_TIMESTAMP(COALESCE(NULLIF(getArgument("end_date"), ''),  date_sub(CURRENT_DATE(),1)),'yyyy-MM-dd')*1000
)

-- COMMAND ----------

merge into dataprep_safety.cm_power_ranges as target
using cm_power_ranges_updates as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.linked_cm_id = updates.linked_cm_id
and target.start_ms = updates.start_ms
when matched then update set *
when not matched then insert * ;
