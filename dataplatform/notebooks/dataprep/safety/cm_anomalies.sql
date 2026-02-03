-- Databricks notebook source
create or replace temporary view osdanomalyevent as (
  select
    osdanomalyevent.org_id,
    osdanomalyevent.object_id,
    osdanomalyevent.date,
    osdanomalyevent.time,
    osdanomalyevent.value.proto_value.anomaly_event.description,
    osdanomalyevent.value.proto_value.anomaly_event.service_name
  from kinesisstats.osdanomalyevent
  where
    osdanomalyevent.value.is_databreak = 'false' and
    osdanomalyevent.value.is_end = 'false'
    and date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
)
-- COMMAND ----------
create or replace temporary view cm_anomalies as (
  select
  cm_vg_intervals.org_id,
  cm_vg_intervals.device_id,
  cm_vg_intervals.cm_device_id,
  cm_vg_intervals.cm_product_id,
  cm_vg_intervals.product_id,
  cm_vg_intervals.start_ms,
  cm_vg_intervals.end_ms,
  cm_vg_intervals.date,
  count(osdanomalyevent.description) as cm_total_anomalies,
  sum(case when osdanomalyevent.description LIKE '%runtime error%' THEN 1 ELSE 0 end) as cm_runtime_errors,
  sum(case when osdanomalyevent.description LIKE '%kernel%' THEN 1 ELSE 0 end) as cm_kernel_errors,
  sum(case when osdanomalyevent.description LIKE '%panic: recording done returned err: Event -1 received%' THEN 1 ELSE 0 end) as cm_negative_one_errors,
  count(substring_index(osdanomalyevent.service_name,':',2)) as cm_anomaly_event_service_count
from (SELECT * FROM dataprep_safety.cm_vg_intervals
      WHERE date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) cm_vg_intervals
join osdanomalyevent on
  osdanomalyevent.object_id = cm_vg_intervals.cm_device_id and
  osdanomalyevent.org_id = cm_vg_intervals.org_id and
  osdanomalyevent.time >= cm_vg_intervals.start_ms and
  osdanomalyevent.time <= cm_vg_intervals.end_ms and
  osdanomalyevent.date = cm_vg_intervals.date
group by
    cm_vg_intervals.org_id,
    cm_vg_intervals.device_id,
    cm_vg_intervals.cm_device_id,
    cm_vg_intervals.cm_product_id,
    cm_vg_intervals.product_id,
    cm_vg_intervals.start_ms,
    cm_vg_intervals.end_ms,
    cm_vg_intervals.date
);
-- COMMAND ----------

create table if not exists dataprep_safety.cm_anomalies
using delta
partitioned by (date)
as
select *
from cm_anomalies

-- COMMAND ----------
--Updated date stop to backfill missing information from sequential job failues in November
create or replace temporary view anomalies_updates as (
  select *
  from cm_anomalies
  where date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
    and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

merge into dataprep_safety.cm_anomalies as target
using anomalies_updates as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.cm_device_id = updates.cm_device_id
and target.start_ms = updates.start_ms
and target.end_ms = updates.end_ms
when matched then update set *
when not matched then insert * ;
