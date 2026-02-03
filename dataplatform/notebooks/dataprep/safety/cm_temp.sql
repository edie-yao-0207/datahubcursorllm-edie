-- Databricks notebook source
create or replace temporary view osdcm3x_temps as (
  select
    osdcm3xthermalsensors.date,
    osdcm3xthermalsensors.org_id,
    osdcm3xthermalsensors.object_id,
    osdcm3xthermalsensors.time,
    osdcm3xthermalsensors.value.proto_value.cm3x_tsens.msm_therm_adc,
    osdcm3xthermalsensors.value.proto_value.cm3x_tsens.cpu0_gold_usr,
    osdcm3xthermalsensors.value.proto_value.cm3x_tsens.battery
  from kinesisstats.osdcm3xthermalsensors
  where
     osdcm3xthermalsensors.value.is_databreak = 'false' and
     osdcm3xthermalsensors.value.is_end = 'false'
    and date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
)

-- COMMAND ----------
create or replace temporary view cm_interval_temps AS
(
select
  cm_vg_intervals.date,
  cm_vg_intervals.org_id,
  cm_vg_intervals.device_id,
  cm_vg_intervals.cm_device_id,
  cm_vg_intervals.product_id,
  cm_vg_intervals.cm_product_id,
  cm_vg_intervals.start_ms,
  cm_vg_intervals.end_ms,
  max(osdcm3x_temps.msm_therm_adc)/1000 as msm_max_temp_celsius,
  avg(osdcm3x_temps.msm_therm_adc)/1000 as msm_avg_temp_celsius,
  max(osdcm3x_temps.cpu0_gold_usr)/1000 as cpu_gold_max_temp_celsius,
  avg(osdcm3x_temps.cpu0_gold_usr)/1000 as cpu_gold_avg_temp_celsius,
  max(osdcm3x_temps.battery)/1000 as battery_max_temp_celsius,
  avg(osdcm3x_temps.battery)/1000 as battery_avg_temp_celsius
from (SELECT * FROM dataprep_safety.cm_vg_intervals
      WHERE date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) cm_vg_intervals
join osdcm3x_temps on
  osdcm3x_temps.object_id = cm_vg_intervals.cm_device_id and
  osdcm3x_temps.org_id = cm_vg_intervals.org_id and
  osdcm3x_temps.time >= cm_vg_intervals.start_ms and
  osdcm3x_temps.time <= cm_vg_intervals.end_ms and
  osdcm3x_temps.date = cm_vg_intervals.date
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

create or replace temporary view cm_interval_temps_updates AS
(
  select *
  from cm_interval_temps
  where date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),3)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

merge into dataprep_safety.cm_temps as target
using cm_interval_temps_updates as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.cm_device_id = updates.cm_device_id
and target.start_ms = updates.start_ms
and target.end_ms = updates.end_ms
and target.date = updates.date
when matched then update set *
when not matched then insert * ;
