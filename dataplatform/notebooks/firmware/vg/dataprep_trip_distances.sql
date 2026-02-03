-- Databricks notebook source
-- Grab distance between trips for VG34s and Thors
-- At small distances the difference between eucleidian distance and haversine are within 0.5% (https://jonisalonen.com/2014/computing-distance-between-coordinates-can-be-simple-and-fast/)
-- Since harversine is expensive to run, I will use eucleidian approximation for the distance between locations.
create or replace temp view device_trip_gaps as (
  select 
    t.date,
    t.org_id,
    t.device_id,
    gw.product_id,
    SQRT(power((t.proto.start.latitude - t.proto.prev_end.latitude),2) + power(((t.proto.start.longitude - t.proto.prev_end.longitude)*cos(t.proto.start.latitude)),2))*110.25 as trip_gap_km
  from trips2db_shards.trips as t
  join clouddb.devices as d on 
    t.device_id = d.id
    and t.org_id = d.org_id
  join clouddb.gateways as gw on
    d.org_id = gw.org_id
    and d.id = gw.device_id
  where
    gw.product_id in (24, --VG34
                      35, --VG34EU
                      53, --VG54
                      89, --VG54EU
                      178) --VG55(NA/EU/FN)
    and date >= date_sub(current_date(),10)
    and t.version = 101
)

-- COMMAND ----------

create or replace temp view device_trip_percentiles as (
  select
    date,
    org_id,
    device_id,
    percentile_approx(trip_gap_km, 0.90) as 90th_percentile,
    percentile_approx(trip_gap_km, 0.75) as 75th_percentile,
    percentile_approx(trip_gap_km, 0.50) as 50th_percentile,
    count(*) as total_trips
  from device_trip_gaps
  group by
    date,
    org_id,
    device_id
)

-- COMMAND ----------

create table if not exists data_analytics.dataprep_trip_distances
using delta
partitioned by (date)
select * from device_trip_percentiles

-- COMMAND ----------

create temp view device_trip_percentiles_updates as (
  select *
  from device_trip_percentiles
  where date >= date_sub(current_date(),10)
);
  
merge into data_analytics.dataprep_trip_distances as target 
using device_trip_percentiles_updates as updates 
on target.date = updates.date
and target.org_id = updates.org_id
and target.device_id = updates.device_id
when matched then update set *
when not matched then insert *
