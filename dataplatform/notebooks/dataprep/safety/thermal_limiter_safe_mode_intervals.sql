-- Databricks notebook source

SELECT assert_true(getArgument("start_date") >= '2016-01-01' OR getArgument("start_date") = '') -- bad data for dates earlier than 2016-01-01

-- COMMAND ----------

-- Pull in entries from osdthermallimiterstate start_date <= x <= end_date.
-- If no start_date, end_date specified, use date window ending day before query run.
-- Note: Open-ended queries will be closed later in this notebook using either
--   CURRENT_DATE() or date_add(end_date, 1).
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
  -- databreak?
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

-- safemode_hist temp view creates entries representing state transitions.
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

-- Where a given org_id, cm_device_id has no later cur_time (i.e., last timestamp),
--   end_ms will be null.
create or replace temporary view safemode_states as (
  select
  cm_device_id,
  org_id,
  cur_time as start_ms,
  cur_state as is_safe_mode,
  lead(cur_time) over (partition by org_id, cm_device_id order by cur_time) as end_ms
from safemode_hist
);

-- COMMAND ----------

-- Closes open intervals (end_ms is null) with either: end_date + 1 at 00:00 or current day 00:00.
-- See temp view cm_safe_mode for basis of end_ms calculations.
create or replace temporary view cm_safe_mode_intervals as (
  select org_id,
  cm_device_id,
  is_safe_mode,
  start_ms,
  -- end_ms -> end_date + 1 -> end of previous day (current day 00:00)
  COALESCE(end_ms,
    UNIX_TIMESTAMP(COALESCE(NULLIF(date_add(getArgument("end_date"), 1), ''),
      CURRENT_DATE()),'yyyy-MM-dd')*1000) AS end_ms
from safemode_states
where start_ms != 0
);

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW cm_safe_mode_intervals_dates AS (
  SELECT *,
    date(from_unixtime(start_ms/1000, "yyyy-MM-dd HH:mm:ss")) AS date_start,
    date(from_unixtime(end_ms/1000, "yyyy-MM-dd HH:mm:ss")) AS date_end
  FROM cm_safe_mode_intervals
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep_safety.cm_safe_mode_intervals
USING DELTA
PARTITIONED BY (date_start)
COMMENT 'Logs safe mode intervals for cm devices. Created for use in Safety Firmware metrics.

Note: Once the last safe mode state for a cm pulled from kinesisstats.osdthermallimiterstate is older than the start_date, the interval for that cm will close with an end date of the last date that entry was queried. E.g., if the last safe mode state for a cm was logged in osdthermallimiterstate on Feb 15, then once the query window updates to Feb 16 - 23 on Feb 24, the safe mode will stop being updated for that cm until a change of state is recorded in osdthermallimiterstate. The last safe mode interval in this table for that cm will be from Feb 15 to Feb 22.'
AS SELECT * FROM cm_safe_mode_intervals_dates;

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW cm_safe_mode_intervals_updates AS (
  SELECT *
  FROM cm_safe_mode_intervals_dates
  WHERE date_end BETWEEN COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(CURRENT_DATE(),8))
    AND COALESCE(NULLIF(getArgument("end_date"), ''), CURRENT_DATE())
);

-- COMMAND ----------

MERGE INTO dataprep_safety.cm_safe_mode_intervals AS target
USING cm_safe_mode_intervals_updates AS updates
ON target.org_id = updates.org_id
AND target.cm_device_id = updates.cm_device_id
AND target.start_ms = updates.start_ms
-- Omit join on end_ms since open intervals will substitute current_date+1 for end_ms and will thus constantly change.
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
