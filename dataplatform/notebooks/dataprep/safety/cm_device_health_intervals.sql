-- Databricks notebook source
SET spark.databricks.delta.schema.autoMerge.enabled = TRUE;

create or replace temporary view cm_device_health_intervals as
  (select distinct di.*,
  cm_hb.first_heartbeat_date as cm_first_heartbeat_date,
  cm_hb.first_heartbeat_ms as cm_first_heartbeat_ms,
  cm_hb.last_heartbeat_date as cm_last_heartbeat_date,
  cm_hb.last_heartbeat_ms as cm_last_heartbeat_ms,
  vg_hb.first_heartbeat_date as vg_first_heartbeat_date,
  vg_hb.first_heartbeat_ms as vg_first_heartbeat_ms,
  vg_hb.last_heartbeat_date as vg_last_heartbeat_date,
  vg_hb.last_heartbeat_ms as vg_last_heartbeat_ms,
  dbb.cm_build,
  coalesce(dbb.cm_bootcount, 0) as cm_bootcount,
  dbb.vg_build,
  coalesce(dbb.vg_bootcount, 0) as vg_bootcount,
  coalesce(a.cm_total_anomalies, 0) as cm_total_anomalies,
  coalesce(a.cm_anomaly_event_service_count,0) as cm_anomaly_event_service_count,
  coalesce(a.cm_runtime_errors, 0) as cm_runtime_errors,
  coalesce(a.cm_kernel_errors, 0) as cm_kernel_errors,
  coalesce(a.cm_negative_one_errors, 0) as cm_negative_one_errors,
  t.msm_max_temp_celsius,
  t.msm_avg_temp_celsius,
  t.cpu_gold_max_temp_celsius,
  t.cpu_gold_avg_temp_celsius,
  t.battery_max_temp_celsius,
  t.battery_avg_temp_celsius,
  vg_temps.max_temp_celsius as vg_max_temp_celsius,
  vg_temps.avg_temp_celsius as vg_avg_temp_celsius,
  rd.grace_recording_duration,
  rd.interval_connected_duration,
  rd.recording_duration_ms,
  td.target_recording_duration_ms,
  td.grace_target_recording_duration_ms,
  coalesce(he.harsh_event_count,0) as harsh_event_count,
  coalesce(he.successful_file_upload_count,0) as successful_file_upload_count,
  cod.overheated_duration_ms,
  cod.overheated_count,
  csmd.safe_mode_duration_ms,
  csmd.safe_mode_count,
  cslad.speed_limit_available_duration_ms
  from (SELECT * FROM dataprep_safety.cm_vg_intervals
        WHERE date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) as di
  left join (SELECT * FROM dataprep_safety.device_build_and_bootcount
             WHERE date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) dbb on
    di.org_id = dbb.org_id and
    di.device_id = dbb.device_id and
    di.cm_device_id = dbb.cm_device_id and
    dbb.start_ms = di.start_ms and
    dbb.end_ms = di.end_ms
  left join (SELECT * FROM dataprep_safety.cm_anomalies
             WHERE date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) a on
    di.org_id = a.org_id and
    di.device_id = a.device_id and
    di.cm_device_id = a.cm_device_id and
    a.start_ms = di.start_ms and
    a.end_ms = di.end_ms
  left join (SELECT * FROM dataprep_safety.cm_temps
             WHERE date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) t on
    di.org_id = t.org_id and
    di.device_id = t.device_id and
    di.cm_device_id = t.cm_device_id and
    t.start_ms = di.start_ms and
    t.end_ms = di.end_ms
  left join (SELECT * FROM dataprep_safety.cm_recording_durations
             WHERE date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) rd on
    di.org_id = rd.org_id and
    di.device_id = rd.device_id and
    di.cm_device_id = rd.cm_device_id and
    rd.start_ms = di.start_ms and
    rd.end_ms = di.end_ms
  left join (SELECT * FROM dataprep_firmware.cm_target_recording_durations
             WHERE date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) td on
    di.org_id = td.org_id and
    di.device_id = td.device_id and
    di.cm_device_id = td.cm_device_id and
    td.start_ms = di.start_ms and
    td.end_ms = di.end_ms
  left join (SELECT * FROM dataprep_safety.safety_harsh_events_intervals
             WHERE date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) as he on
    di.org_id = he.org_id and
    di.device_id = he.device_id and
    di.cm_device_id = he.cm_device_id and
    he.start_ms = di.start_ms and
    he.end_ms = di.end_ms
  left join (SELECT * FROM dataprep_safety.vg_air_temp
             WHERE date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) vg_temps on
    di.org_id = vg_temps.org_id and
    di.device_id = vg_temps.device_id and
    di.cm_device_id = vg_temps.cm_device_id and
    vg_temps.start_ms = di.start_ms and
    vg_temps.end_ms = di.end_ms
  left join dataprep.device_heartbeats_extended cm_hb on
    di.org_id = cm_hb.org_id and
    di.cm_device_id = cm_hb.device_id
  left join dataprep.device_heartbeats_extended vg_hb on
    di.org_id = vg_hb.org_id and
    di.device_id = vg_hb.device_id
  left join (SELECT * FROM dataprep_safety.cm_overheating_durations
             WHERE date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) cod on
    di.org_id = cod.org_id and
    di.device_id = cod.device_id and
    di.cm_device_id = cod.cm_device_id and
    di.start_ms = cod.trip_start_ms and
    di.end_ms = cod.trip_end_ms
  left join (SELECT * FROM dataprep_safety.cm_safe_mode_durations
             WHERE date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) csmd on
    di.org_id = csmd.org_id and
    di.device_id = csmd.device_id and
    di.cm_device_id = csmd.cm_device_id and
    di.start_ms = csmd.trip_start_ms and
    di.end_ms = csmd.trip_end_ms
  left join (SELECT * FROM dataprep_safety.cm_speed_limit_available_durations
             WHERE date between COALESCE(NULLIF(DATE_SUB(getArgument("start_date"),5), ''),  date_sub(CURRENT_DATE(),10)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())) cslad on
    di.org_id = cslad.org_id and
    di.device_id = cslad.device_id and
    di.cm_device_id = cslad.cm_device_id and
    di.start_ms = cslad.trip_start_ms and
    di.end_ms = cslad.trip_end_ms
);

-- COMMAND ----------
create or replace temporary view cm_device_health_intervals_updates as (
  select DISTINCT * from cm_device_health_intervals where date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),5)) and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
)

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS dataprep_safety.cm_device_health_intervals
USING DELTA
PARTITIONED BY (date) AS
SELECT DISTINCT * FROM cm_device_health_intervals;

-- COMMAND ----------
merge into dataprep_safety.cm_device_health_intervals as target
using cm_device_health_intervals_updates as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.cm_device_id = updates.cm_device_id
and target.start_ms = updates.start_ms
and target.end_ms = updates.end_ms
and target.date = updates.date
when matched then update set *
when not matched then insert * ;

-- METADATA ---------
ALTER TABLE dataprep_safety.cm_device_health_intervals
SET TBLPROPERTIES ('comment' = 'These metrics use dataprep_safety.cm_vg_intervals table as the base table, and then joins the the other cm metrics onto the intervals. The other metrics have already been aggregated an interval level individually so we can just join on CM info and start_ms & end_ms of the intervals.');

ALTER TABLE dataprep_safety.cm_device_health_intervals
CHANGE org_id
COMMENT 'The Samsara Cloud dashboard ID that the data belongs to';

ALTER TABLE dataprep_safety.cm_device_health_intervals
CHANGE org_name
COMMENT 'The name of the org that the data belongs to';

ALTER TABLE dataprep_safety.cm_device_health_intervals
CHANGE org_type
COMMENT 'The organization type that these devices belong to. This would be Internal, or Customer.';

ALTER TABLE dataprep_safety.cm_device_health_intervals
CHANGE cm_device_id
COMMENT 'The device ID of the CM we are reporting metrics for';

ALTER TABLE dataprep_safety.cm_device_health_intervals
CHANGE product_id
COMMENT 'The product ID of the VG the CM is paired to. The product ID mapping can be found in the products.go file in the codebase';

ALTER TABLE dataprep_safety.cm_device_health_intervals
CHANGE cm_product_id
COMMENT 'The product ID of the CM product we are reporting daily metrics for';

ALTER TABLE dataprep_safety.cm_device_health_intervals
CHANGE device_id
COMMENT 'The device ID of the VG that the CM is paired to';

ALTER TABLE dataprep_safety.cm_device_health_intervals
CHANGE target_recording_duration_ms
COMMENT 'The duration (ms) that CM Recording Manager determines the CM should be recording for non parking mode reasons.';

ALTER TABLE dataprep_safety.cm_device_health_intervals
CHANGE grace_target_recording_duration_ms
COMMENT 'The duration (ms) that CM Recording Manager determines the CM should be recording for non parking mode reasons, adjusted such that if the
recording intersects the first 90 seconds of the interval, assume that it starts since the beginning of the interval.';
