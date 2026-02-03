-- Databricks notebook source
create or replace temp view crashes_with_trip_start_ms as(
  select se.date, se.org_id, se.device_id, se.event_ms, se.detail_proto.hidden_to_customer, se.trip_start_ms, t.end_ms as trip_end_ms from safetydb_shards.safety_events as se
  left join trips2db_shards.trips t
  on t.org_id = se.org_id
  and t.device_id = se.device_id
  and t.proto.start.time = se.trip_start_ms
  and t.version = 101
  where se.detail_proto.accel_type in (5, 28) --crash, rollover
  and se.detail_proto.ingestion_tag = 1 -- HEv2
  and se.trip_start_ms > 0
  and year(se.date) >=2022
)

-- COMMAND ----------

create or replace temp view surfaced_crashes as (
  select date, org_id, device_id, event_ms, hidden_to_customer, trip_start_ms, trip_end_ms, event_ms - trip_end_ms as ms_after_trip_end from crashes_with_trip_start_ms
)

-- COMMAND ----------

create or replace temp view crashes_without_trip_start_ms as (
  select se.date, se.org_id, se.device_id, se.event_ms, se.detail_proto.hidden_to_customer, se.trip_start_ms from safetydb_shards.safety_events as se
  where se.detail_proto.accel_type in (5, 28) --crash, rollover
  and se.detail_proto.ingestion_tag = 1 -- HEv2
  and se.trip_start_ms <= 0
  and year(se.date) >=2022
)

-- COMMAND ----------

-- for each org, device get the start, end and next start time for a trip, if there is no end trip, it's null
create or replace temp view trips_info_with_next_trip_time as (
  select
  date,
  device_id,
  org_id,
  start_ms,
  end_ms,
  lead(start_ms) over (partition by org_id, device_id order by start_ms) as next_start_time
  from trips2db_shards.trips
  where year(date) >=2022
  and version = 101
);

-- COMMAND ----------

create or replace temp view crashes_not_surfaced as (
  select /*+ RANGE_JOIN(c, 10800000) */ -- three hour bin
  c.*,
  t.end_ms as trip_end_ms,
  (c.event_ms - t.end_ms) as ms_after_trip_end
  -- (t.next_start_time - c.event_ms)/1000/60 as min_before_next_trip
  from trips_info_with_next_trip_time t
  join crashes_without_trip_start_ms c on
    c.org_id = t.org_id
    and c.device_id = t.device_id
    and c.event_ms > t.end_ms and (c.event_ms < t.next_start_time or t.next_start_time is null)
    -- here we include the off-trip crashes that does not have a following trip
)

-- COMMAND ----------

create table if not exists
dataprep_safety.all_crash_events_since_2022
using delta
partitioned by (date)
as (
  select * from surfaced_crashes
  union all
  select * from crashes_not_surfaced
)
