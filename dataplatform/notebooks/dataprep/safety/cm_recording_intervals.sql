create or replace temporary view osddashcamstate as (
   select
     org_id,
     object_id,
     time,
     date,
     value.int_value
   from kinesisstats.osddashcamstate
   where
      osddashcamstate.value.is_databreak = false and
      osddashcamstate.value.is_end = false and
      date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
)

-- COMMAND ----------
create or replace temporary view cm_recordings as
(
  select
  cm_linked_vgs.org_id,
  cm_linked_vgs.linked_cm_id,
  osddashcamstate.time,
  case
    when osddashcamstate.int_value = 1 then 1
    else 0 end
    as is_recording,
  osddashcamstate.date
  from dataprep_safety.cm_linked_vgs as cm_linked_vgs
  join osddashcamstate on
    cm_linked_vgs.org_id = osddashcamstate.org_id and
    cm_linked_vgs.linked_cm_id = osddashcamstate.object_id
);

-- COMMAND ----------
create or replace temporary view cm_recording_intervals as
with pairs as (
select
  org_id,
  linked_cm_id,
  lag(time) over (partition by org_id, linked_cm_id order by time asc) as prev_time, --use this to see if the pair is the first item
  lag(is_recording) OVER (PARTITION BY org_id, linked_cm_id ORDER BY time ASC) AS prev_is_recording,
  time AS cur_time,
  is_recording AS cur_is_recording
  FROM cm_recordings
),

-- filter down to points with recording change
transitions as (
select
  org_id,
  linked_cm_id,
  prev_time,
  prev_is_recording,
  cur_time,
  cur_is_recording
from pairs
where
  cur_is_recording != prev_is_recording
  or (cur_time - prev_time) >= 90 * 1000
  or prev_time is null
),

intervals AS (
  SELECT
    org_id,
    linked_cm_id,
    cur_time AS start_ms,
    cur_is_recording AS is_recording,
    lead(cur_time) OVER (PARTITION BY org_id, linked_cm_id ORDER BY cur_time) AS end_ms
  FROM
    transitions
),

recording_intervals_closed AS (
  SELECT
    org_id,
    linked_cm_id,
    is_recording,
    start_ms,
    end_ms
  FROM intervals
  WHERE end_ms IS NOT NULL
),
-- Select the latest dashcam stat per device. This will be used to close
-- the last open interval.
latest_recording AS (
    SELECT
      org_id,
      linked_cm_id,
      MAX(time) as latest_time
    FROM
      cm_recordings
    GROUP BY
      org_id,
      linked_cm_id
),
-- construct a closed interval out of the last open interval. Since there is
-- no state change for the last interval, we do not want to drop it. Instead
-- we overwrite the NULL end_ms with the time_ms of the last datapoint.
recording_intervals_open AS (
  SELECT
    iv.org_id,
    iv.linked_cm_id,
    iv.is_recording,
    iv.start_ms,
    latest.latest_time as end_ms
  FROM
    intervals as iv
  INNER JOIN latest_recording as latest
    ON iv.org_id = latest.org_id
    AND iv.linked_cm_id = latest.linked_cm_id
  WHERE
    end_ms IS NULL
),

recording_intervals AS (
  SELECT
    *
  FROM
    recording_intervals_closed
  UNION
  SELECT
    *
  FROM
    recording_intervals_open
)

select * from recording_intervals

-- COMMAND ----------

create or replace temporary view cm_disconnected_intervals_raw as (
   select
    a.org_id,
    a.device_id,
    a.cm_device_id,
    c.product_id,
    d.product_id as cm_product_id,
    a.start_ms,
    a.end_ms,
    a.duration_ms,
    a.date,
    a.on_trip,
    min(b.start_ms) as disconnected_start,
    max(b.end_ms) as disconnected_end,
    sum
    (
      case
      when b.end_ms is not null and b.start_ms is not null then least(b.end_ms, a.end_ms) - greatest(b.start_ms, a.start_ms)
      else 0
      end
    ) as interval_disconnected_duration_raw
  from dataprep_safety.cm_vg_intervals as a
  left join dataprep_safety.cm_physically_disconnected_ranges as b on
    a.org_id = b.org_id and
    a.device_id = b.device_id and
    a.cm_device_id = b.linked_cm_id and
    not (b.start_ms >= a.end_ms or b.end_ms <= a.start_ms)
  left join productsdb.devices c
    ON a.device_id = c.id
  left join productsdb.devices d
    ON a.cm_device_id = d.id
  where
    a.date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
  group by
     a.org_id,
    a.device_id,
    a.cm_device_id,
    d.product_id,
    c.product_id,
    a.start_ms,
    a.end_ms,
    a.duration_ms,
    a.date,
    a.on_trip
)

-- COMMAND ----------

create or replace temporary view cm_disconnected_intervals as (
  select *,
  duration_ms - coalesce(interval_disconnected_duration_raw, 0) as interval_connected_duration
  from cm_disconnected_intervals_raw
)

-- COMMAND ----------

create or replace temporary view cm_recording_durations as (
  select
    a.org_id,
    a.device_id,
    a.cm_device_id,
    a.product_id,
    a.cm_product_id,
    a.start_ms,
    a.end_ms,
    a.duration_ms,
    a.date,
    a.interval_connected_duration,
    greatest(min(b.start_ms), a.start_ms) as interval_recording_start_ms,
    sum
    (
      case
        when b.start_ms <= a.start_ms and b.end_ms >= a.end_ms then a.end_ms - a.start_ms
        when b.start_ms >= a.start_ms and b.end_ms <= a.end_ms then b.end_ms - b.start_ms
        when b.start_ms <= a.start_ms and b.end_ms <= a.end_ms then b.end_ms - a.start_ms
        when b.start_ms >= a.start_ms and b.end_ms >= a.end_ms then a.end_ms - b.start_ms
        else 0
      end
    ) as recording_duration_ms
    from cm_disconnected_intervals a
    left join cm_recording_intervals b on
      a.org_id = b.org_id and
      a.cm_device_id = b.linked_cm_id and
      not (b.start_ms >= a.end_ms or b.end_ms <= a.start_ms) and
      b.is_recording = 1 and
      b.end_ms is not null and
      b.start_ms is not null
    group by
      a.org_id,
      a.device_id,
      a.cm_device_id,
      a.product_id,
      a.cm_product_id,
      a.start_ms,
      a.end_ms,
      a.duration_ms,
      a.date,
      a.interval_connected_duration
);


-- COMMAND ----------

create or replace temporary view cm_recording_durations_grace as (
  select
    org_id,
    device_id,
    cm_device_id,
    product_id,
    cm_product_id,
    start_ms,
    end_ms,
    duration_ms,
    date,
    interval_connected_duration,
    case when interval_recording_start_ms < start_ms + 90*1000 then recording_duration_ms + interval_recording_start_ms - start_ms else recording_duration_ms end as grace_recording_duration,
    recording_duration_ms
  from cm_recording_durations
);

-- COMMAND ----------

create table if not exists dataprep_safety.cm_recording_durations
using delta
partitioned by (date)
as
select * from cm_recording_durations_grace

-- COMMAND ----------

create or replace temporary view cm_recording_durations_updates as (
  select * from cm_recording_durations_grace where date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),5)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
)

-- COMMAND ----------

merge into dataprep_safety.cm_recording_durations as target
using cm_recording_durations_updates as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.cm_device_id = updates.cm_device_id
and target.start_ms = updates.start_ms
and target.end_ms = updates.end_ms
and target.date = updates.date
when matched then update set *
when not matched then insert * ;

-- METADATA --------

ALTER TABLE dataprep_safety.cm_recording_durations
SET TBLPROPERTIES ('comment' = 'This table contains recording uptime durations for
each interval in dataprep_safety.cm_vgs_intervals. The final table
contains how long the CM was recording for each interval. We calculate
this in the following way:
1. Taking the dashcam connected object stats from
dataprep_safety.cm_vgs_intervals, we merge these together to create
intervals for which the CM was recording. We define a change in
recording state if the next object stat for the CM is different from the
previous one, or if the next object stat timestamp is at least 90
seconds after the previous one since this object stat is reported
heartbeat style.
2. Then we intersect the intervals where the CM was physically
disconnected with the cm_vg_intervals to find out how long for each
interval the CM was physically disconnected.
3. We intersect the recording intervals with the cm_vg_intervals to find
out how long the CM was recording for each cm_vg_interval.
4. Finally we take the grace period into account. If the CM recording
interval intersects the first 90 seconds of the trip, then we assume the
CM was recording since the start of the trip, and adjust the duration
accordingly.

For recording uptime during trips, one can just divide the grace
recording duration by the connected duration where on_trip = true to get
the recording uptime.');

ALTER TABLE dataprep_safety.cm_recording_durations
CHANGE org_id
COMMENT 'The Samsara Cloud dashboard ID that the data belongs to';


ALTER TABLE dataprep_safety.cm_recording_durations
CHANGE cm_device_id
COMMENT 'The device ID of the CM we are reporting metrics for';

ALTER TABLE dataprep_safety.cm_recording_durations
CHANGE product_id
COMMENT 'The product ID of the VG the CM is paired to. The product ID mapping can be found in the products.go file in the codebase';

ALTER TABLE dataprep_safety.cm_recording_durations
CHANGE cm_product_id
COMMENT 'The product ID of the CM product we are reporting daily metrics for';

ALTER TABLE dataprep_safety.cm_recording_durations
CHANGE device_id
COMMENT 'The device ID of the VG that the CM is paired to';

ALTER TABLE dataprep_safety.cm_recording_durations
CHANGE date
COMMENT 'The date that we are reporting the metrics for.';

ALTER TABLE dataprep_safety.cm_recording_durations
CHANGE start_ms
COMMENT 'The start timestamp (ms) of the CM VG interval that we are reporting the metrics for.';

ALTER TABLE dataprep_safety.cm_recording_durations
CHANGE end_ms
COMMENT 'The end timestamp (ms) of the CM VG interval that we are reporting the metrics for.';

ALTER TABLE dataprep_safety.cm_recording_durations
CHANGE grace_recording_duration
COMMENT 'The recording duration (ms) that includes the grace period. If the CM recording
interval intersects the first 90 seconds of the trip, then we assume the
CM was recording since the start of the trip, and adjust the duration
accordingly.';
