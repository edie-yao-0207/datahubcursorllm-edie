create or replace temp view cohort as (
	select
    org_id,
    cm_device_id
	from dataprep_firmware.cm_device_daily_metadata
	where date = least(('{{ date_range.end }}'), (select string(max(date)) from dataprep_firmware.cm_device_daily_metadata))
    and ('all' in ({{ org_type }}) or lower(org_type) in ({{ org_type }}))
    and ('all' in ({{ org_id }}) or org_id in ({{ org_id }}))
    and ('all' in ({{ vg_product_name }}) or vg_product_name in ({{ vg_product_name }}))
    and ('all' in ({{ vg_firmware }}) or last_reported_vg_build in ({{ vg_firmware }}))
    and ('all' in ({{ cm_product_name }}) or cm_product_name in ({{ cm_product_name }}))
    and ('all' in ({{ cm_firmware }}) or last_reported_cm_build in ({{ cm_firmware }}))
    and ('all' in ({{ cm_product_program_id }}) or cm_product_program_id in ({{ cm_product_program_id }}))
    and ('all' in ({{ cm_rollout_stage_id }}) or cm_rollout_stage_id in ({{ cm_rollout_stage_id }}))
    and ('all' in ({{ cm_feature_flag }}) or exists(cm_feature_flags, x -> concat(x.key, ":", x.value) in ({{ cm_feature_flag }})))
    and ('all' in ({{ vg_feature_flag }}) or exists(vg_feature_flags, x -> concat(x.key, ":", x.value) in ({{ vg_feature_flag }})))
    and ('all' in ({{ org_cell }}) or org_cell in ({{ org_cell }}))
    and ('all' in ({{ org_release_track }}) or org_release_track in ({{ org_release_track }}))
);

create or replace temp view daily_cohort as (
  select
    a.date,
    a.org_id,
    a.cm_device_id,
    a.vg_device_id
  from dataprep_firmware.cm_device_daily_metadata as a
  join cohort as b
    on a.org_id = b.org_id
    and a.cm_device_id = b.cm_device_id
  where a.date between ('{{ date_range.start }}') and ('{{ date_range.end }}')
    and ('all' in ({{ vg_firmware }}) or a.last_reported_vg_build in ({{ vg_firmware }}))
    and ('all' in ({{ cm_firmware }}) or a.last_reported_cm_build in ({{ cm_firmware }}))
);

-- Compute recording uptime for each device
create or replace temporary view cm_recording_uptime as (
  SELECT
    a.date,
    a.device_id,
    a.cm_device_id,
    a.org_id,
    a.last_reported_cm_build,
    a.cm_last_heartbeat_date,
    a.total_trip_recording_duration_ms,
    a.total_trip_grace_recording_duration_ms,
    a.total_trip_connected_duration_ms,
    a.total_trip_duration_ms,
    (a.total_trip_grace_recording_duration_ms / a.total_trip_duration_ms) as recording_uptime_pct,
    (a.total_trip_grace_recording_duration_ms / a.total_trip_connected_duration_ms) as recording_uptime_connected_pct
  FROM
    dataprep_firmware.cm_device_health_daily as a
  JOIN
    daily_cohort as b
  ON
    a.cm_device_id = b.cm_device_id
    and a.org_id = b.org_id
    and a.date = b.date
  WHERE
    total_trip_duration_ms > 600000
    AND cm_last_heartbeat_date > date_sub(date, 1) --remove CMs without heartbeat
);

-- Compute total recording uptime
SELECT
  date,
  SUM(total_trip_recording_duration_ms) as total_recording_duration,
  SUM(total_trip_duration_ms) as total_trip_time,
  SUM(total_trip_grace_recording_duration_ms) / sum(total_trip_duration_ms) as total_recording_uptime,
  SUM(total_trip_grace_recording_duration_ms) / SUM(total_trip_connected_duration_ms) as total_recording_uptime_connected,
  percentile_approx(recording_uptime_connected_pct, 0.01) as recording_uptime_connected_p99,
  percentile_approx(recording_uptime_pct, 0.50) as recording_uptime_median,
  percentile_approx(recording_uptime_pct, 0.01) as recording_uptime_p99,
  percentile_approx(recording_uptime_pct, 0.001) as recording_uptime_p999
FROM
  cm_recording_uptime
GROUP BY
  1
ORDER BY
  1 desc
