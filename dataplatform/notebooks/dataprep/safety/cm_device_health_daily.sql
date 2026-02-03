-- Databricks notebook source
SET spark.databricks.delta.schema.autoMerge.enabled = TRUE;

-- This view groups all intervals by date,org_id,device_id,product_id,cm_device_id.
create or replace temporary view cm_device_health_daily as
(
  select
    cm_device_health_intervals.date,
    cm_device_health_intervals.org_id,
    max_by(cm_device_health_intervals.org_name, cm_device_health_intervals.end_ms) AS org_name,
    max_by(cm_device_health_intervals.org_type, cm_device_health_intervals.end_ms) AS org_type,
    cm_device_health_intervals.device_id,
    cm_device_health_intervals.product_id,
    max_by(vg_gtwy.variant_id, vg_gtwy.updated_at) AS vg_variant_id,
    first(COALESCE(vg_var.product_version, 'Version 1')) AS vg_product_version,
    cm_device_health_intervals.cm_device_id,
    max_by(cm_device_health_intervals.cm_product_id, cm_device_health_intervals.end_ms) AS cm_product_id,
    max_by(cm_gtwy.variant_id, cm_gtwy.updated_at) AS cm_variant_id,
    max_by(dast.fcw_enabled, dast.date) AS fcw_enabled,
    max_by(dast.fd_enabled, dast.date) AS fd_enabled,
    max_by(dast.ddd_enabled, dast.date) AS ddd_enabled,
    first(cm_first_heartbeat_date) as cm_first_heartbeat_date,
    first(cm_first_heartbeat_ms) as cm_first_heartbeat_ms,
    first(cm_last_heartbeat_date) as cm_last_heartbeat_date,
    first(cm_last_heartbeat_ms) as cm_last_heartbeat_ms,
    first(vg_first_heartbeat_date) as vg_first_heartbeat_date,
    first(vg_first_heartbeat_ms) as vg_first_heartbeat_ms,
    first(vg_last_heartbeat_date) as vg_last_heartbeat_date,
    first(vg_last_heartbeat_ms) as vg_last_heartbeat_ms,
    sum(case when on_trip = true then 1 else 0 end) as total_trips,
    sum(case when on_trip = true then distance_meters else 0 end) as total_distance_meters,
    sum(case when on_trip = true then duration_ms else 0 end) as total_trip_duration_ms,
    sum(case when on_trip = true and interval_connected_duration > 0 then duration_ms else 0 end) as total_trip_connected_duration_ms, --deprecate column
    sum(case when on_trip = true then recording_duration_ms else 0 end) as total_trip_recording_duration_ms,
    sum(case when on_trip = true then grace_recording_duration else 0 end) as total_trip_grace_recording_duration_ms,
    sum(case when on_trip = true then target_recording_duration_ms else 0 end) as total_trip_target_recording_duration_ms,
    sum(case when on_trip = true then grace_target_recording_duration_ms else 0 end) as total_trip_grace_target_recording_duration_ms,
    sum(case when on_trip = true and safe_mode_duration_ms > 0 then duration_ms else 0 end) as total_safe_mode_trip_duration_ms,
    sum(case when on_trip = true and overheated_duration_ms > 0 then duration_ms else 0 end) as total_overheated_trip_duration_ms,
    sum(case when on_trip = true then safe_mode_duration_ms else 0 end) as total_safe_mode_duration_ms,
    sum(case when on_trip = true then overheated_duration_ms else 0 end) as total_overheated_duration_ms,
    sum(case when on_trip = true and safe_mode_duration_ms > 0 then 1 else 0 end) as total_safe_mode_trips,
    sum(case when on_trip = true and overheated_duration_ms > 0 then 1 else 0 end) as total_overheated_trips,
    sum(case when on_trip = true then speed_limit_available_duration_ms else 0 end) as total_speed_limit_available_duration,
    sum(case when on_trip = true and speed_limit_available_duration_ms > 0 then duration_ms else 0 end) as total_speed_limit_trips_duration,
    sum(case when on_trip = true and safe_mode_count > 0 then 1 else 0 end) as total_on_trip_safemode_count,
    sum(case when on_trip = true and overheated_count > 0 then 1 else 0 end) as total_on_trip_overheated_count,
    max((start_ms, cm_build)).cm_build as last_reported_cm_build,
    sum(cm_bootcount) as total_cm_bootcount,
    sum(cm_total_anomalies) as total_cm_anomalies,
    sum(cm_runtime_errors) as total_cm_runtime_errors,
    sum(cm_kernel_errors) as total_cm_kernel_errors,
    sum(cm_negative_one_errors) as total_cm_negative_one_errors,
    max(msm_max_temp_celsius) as daily_max_msm_temp_celsius,
    avg(msm_avg_temp_celsius) as daily_avg_msm_temp_celsius,
    max(cpu_gold_max_temp_celsius) as daily_max_cpu_gold_temp_celsius,
    avg(cpu_gold_avg_temp_celsius) as daily_avg_cpu_gold_temp_celsius,
    max((start_ms, vg_build)).vg_build as last_reported_vg_build,
    sum(vg_bootcount) as total_vg_bootcount,
    max(vg_max_temp_celsius) as daily_max_vg_temp_celsius,
    avg(vg_avg_temp_celsius) as daily_avg_vg_temp_celsius,
    max(battery_max_temp_celsius) as daily_max_battery_temp_celsius,
    avg(battery_avg_temp_celsius) as daily_avg_battery_temp_celsius,
    sum(cm_anomaly_event_service_count) as total_daily_cm_anomaly_event_service_count,
    sum(harsh_event_count) as total_daily_harsh_event_count,
    sum(successful_file_upload_count) as total_daily_successful_file_upload_count,
    sum(safe_mode_count) as total_safe_mode_count,
    sum(overheated_count) as total_overheated_count
  from (SELECT * FROM dataprep_safety.cm_device_health_intervals
        WHERE date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
                       and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
       ) cm_device_health_intervals
  left join (SELECT * FROM dataprep_safety.device_adas_settings_tracking
             WHERE date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
                            and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
            ) as dast on
    cm_device_health_intervals.org_id = dast.org_id and
    cm_device_health_intervals.cm_device_id = dast.device_id and
    cm_device_health_intervals.date = dast.date
  left join productsdb.gateways as cm_gtwy on -- this can be improved to account for historic variant_id changes
    cm_device_health_intervals.org_id = cm_gtwy.org_id and
    cm_device_health_intervals.cm_device_id = cm_gtwy.device_id
  left join productsdb.gateways as vg_gtwy on -- this can be improved to account for historic variant_id changes
    cm_device_health_intervals.org_id = vg_gtwy.org_id and
    cm_device_health_intervals.device_id = vg_gtwy.device_id
  left join data_analytics.vg_hardware_variant_info as vg_var on
    cm_device_health_intervals.org_id = vg_var.org_id and
    cm_device_health_intervals.device_id = vg_var.device_id and
    cm_device_health_intervals.product_id = vg_var.product_id
  group by
    cm_device_health_intervals.date,
    cm_device_health_intervals.org_id,
    cm_device_health_intervals.device_id,
    cm_device_health_intervals.product_id,
    cm_device_health_intervals.cm_device_id
);

-- COMMAND ----------

create table if not exists dataprep_safety.cm_device_health_daily
using delta
partitioned by (date)
as
select * from cm_device_health_daily

-- COMMAND ----------

create or replace temporary view cm_device_health_daily_updates AS (
  select *
  from cm_device_health_daily
  where date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
                 and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

merge into dataprep_safety.cm_device_health_daily as target
using cm_device_health_daily_updates as updates
on target.date = updates.date
and target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.product_id = updates.product_id
and target.cm_device_id = updates.cm_device_id
when matched then update set *
when not matched then insert * ;

ALTER TABLE dataprep_safety.cm_device_health_daily
SET TBLPROPERTIES ('comment' = 'This metric reports the health on a daily basis for each CM<>VG pair.');

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE date
COMMENT 'The date that we are reporting the daily health metrics for.';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE org_id
COMMENT 'The Samsara Cloud dashboard ID that the data belongs to';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE org_name
COMMENT 'The name of the org that the data belongs to';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE org_type
COMMENT 'The organization type that these devices belong to. This would be Internal, or Customer.';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE device_id
COMMENT 'The device ID of the VG that the CM is paired to';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE product_id
COMMENT 'The product ID of the VG the CM is paired to. The product ID mapping can be found in the products.go file in the codebase';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE vg_variant_id
COMMENT 'The product variant of the VG';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE vg_product_version
COMMENT 'The product version of the VG the CM is paired to';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE cm_device_id
COMMENT 'The device ID of the CM we are reporting metrics for';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE cm_product_id
COMMENT 'The product ID of the CM product we are reporting daily metrics for';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE cm_variant_id
COMMENT 'The product variant of the CM';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE fcw_enabled
COMMENT 'Whether the Forward Collision Warning setting is enabled.';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE fd_enabled
COMMENT 'Whether Following Distance setting is enabled.';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE ddd_enabled
COMMENT 'Whether Distracted Driver Detection is enabled.';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE total_trip_target_recording_duration_ms
COMMENT 'The total duration (ms) that CM Recording Manager determines the CM should be recording for non parking mode reasons during trips.';

ALTER TABLE dataprep_safety.cm_device_health_daily
CHANGE total_trip_grace_target_recording_duration_ms
COMMENT 'The total duration (ms) that CM Recording Manager determines the CM should be recording for non parking mode reasons during trips, adjusted
such that if the recording intersects the first 90 seconds of the interval, assume that it starts since the beginning of the interval.';
