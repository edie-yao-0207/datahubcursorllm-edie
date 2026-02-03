-- Databricks notebook source
--Add ambient air temp from the VG
create or replace temporary view osdenginegauge as (
  select
    org_id,
    object_id,
    time,
    date,
    value.proto_value.engine_gauge_event.air_temp_milli_c
  from kinesisstats.osdenginegauge
  where
    osdenginegauge.value.is_databreak = 'false' and
    osdenginegauge.value.is_end = 'false'
    and date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
)

-- COMMAND ----------

--Add ambient air temp from the VG
create or replace temporary view vg_air_temp as (
  select
    cm_vg_intervals.org_id,
    cm_vg_intervals.device_id,
    cm_vg_intervals.cm_device_id,
    cm_vg_intervals.product_id,
    cm_vg_intervals.cm_product_id,
    cm_vg_intervals.start_ms,
    cm_vg_intervals.end_ms,
    cm_vg_intervals.date,
    max(osdenginegauge.air_temp_milli_c)/1000 as max_temp_celsius,
    avg(osdenginegauge.air_temp_milli_c)/1000 as avg_temp_celsius,
    min((osdenginegauge.time, osdenginegauge.air_temp_milli_c)).air_temp_milli_c/1000 as last_temp_celsius
  from (SELECT * FROM dataprep_safety.cm_vg_intervals
        WHERE date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) cm_vg_intervals
  join osdenginegauge on
    osdenginegauge.object_id = cm_vg_intervals.device_id and
    osdenginegauge.org_id = cm_vg_intervals.org_id and
    osdenginegauge.date = cm_vg_intervals.date
  group by
    cm_vg_intervals.org_id,
    cm_vg_intervals.device_id,
    cm_vg_intervals.cm_device_id,
    cm_vg_intervals.product_id,
    cm_vg_intervals.cm_product_id,
    cm_vg_intervals.start_ms,
    cm_vg_intervals.end_ms,
    cm_vg_intervals.date
);

-- COMMAND ----------

create table if not exists dataprep_safety.vg_air_temp
using delta
partitioned by (date)
as
select * from vg_air_temp

-- COMMAND ----------

create or replace temporary view vg_air_temp_updates as
(
  select *
  from vg_air_temp
  where date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

merge into dataprep_safety.vg_air_temp as target
using vg_air_temp_updates as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.cm_device_id = updates.cm_device_id
and target.start_ms = updates.start_ms
and target.end_ms = updates.end_ms
and target.date = updates.date
when matched then update set *
when not matched then insert * ;
