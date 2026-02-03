-- Databricks notebook source

SELECT assert_true(getArgument("start_date") >= '2016-01-01' OR getArgument("start_date") = '') -- bad data for dates earlier than 2016-01-01

-- COMMAND ----------

-- Pull in entries from osdthermallimiterstate start_date <= x <= end_date.
-- If no start_date, end_date specified, use date window ending day before query run.
-- Note: Open-ended queries will be closed later in this notebook using either
--   CURRENT_DATE() or date_add(end_date, 1).
create or replace temporary view cm_overheated_state as (
  select
  cm_linked_vgs.org_id,
  cm_linked_vgs.linked_cm_id as cm_device_id,
  tl.time as timestamp,
  case
    when tl.value.int_value = 1 or tl.value.int_value = 2 then 1
    else 0
  end as is_overheated,
  tl.date
  from dataprep_safety.cm_linked_vgs as cm_linked_vgs
    join kinesisstats.osdthermallimiterstate as tl on
      cm_linked_vgs.org_id = tl.org_id and
      cm_linked_vgs.linked_cm_id = tl.object_id
  where tl.date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),8)) --ensure we get start of interval
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
-- Closes open intervals (end_ms is null) with either: end_date + 1 at 00:00 or current day 00:00.
-- See temp view cm_overheated_state for basis of end_ms calculations.
create or replace temporary view cm_overheated_intervals as (
  select org_id,
  cm_device_id,
  is_overheated,
  start_ms,
  COALESCE(end_ms, -- end_ms -> end_date + 1 -> end of previous day (current day 00:00)
    UNIX_TIMESTAMP(COALESCE(NULLIF(date_add(getArgument("end_date"), 1), ''),
      CURRENT_DATE()),'yyyy-MM-dd')*1000) AS end_ms
  from overheated_states
  where start_ms != 0
);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW cm_overheated_intervals_dates AS (
  SELECT *,
    date(from_unixtime(start_ms/1000, "yyyy-MM-dd HH:mm:ss")) AS date_start,
    date(from_unixtime(end_ms/1000, "yyyy-MM-dd HH:mm:ss")) AS date_end
  FROM cm_overheated_intervals
);

-- COMMAND ----------

create table if not exists dataprep_safety.cm_overheated_intervals
using delta
partitioned by (date_start)
COMMENT 'Logs overheat intervals for cm devices. Created for use in Safety Firmware metrics.

Note: Once the last overheated state for a cm pulled from kinesisstats.osdthermallimiterstate is older than the start_date, the interval for that cm will close with an end date of the last date that entry was queried. E.g., if the last overheated state for a cm was logged in osdthermallimiterstate on Feb 15, then once the query window updates to Feb 16 - 23 on Feb 24, the overheated will stop being updated for that cm until a change of state is recorded in osdthermallimiterstate. The last overheated interval in this table for that cm will be from Feb 15 to Feb 22.'
as select * from cm_overheated_intervals_dates;

-- COMMAND ----------

create or replace temporary view cm_overheated_intervals_updates as (
  select *
  from cm_overheated_intervals_dates
  where date_end between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(CURRENT_DATE(),8))
                 and COALESCE(NULLIF(getArgument("end_date"), ''), CURRENT_DATE())
)

-- COMMAND ----------

merge into dataprep_safety.cm_overheated_intervals as target
using cm_overheated_intervals_updates as updates
on target.org_id = updates.org_id
  and target.cm_device_id = updates.cm_device_id
  and target.start_ms = updates.start_ms
  -- Omit join on end_ms since open intervals will substitute current_date+1 for end_ms and will thus constantly change.
when matched then update set *
when not matched then insert * ;
