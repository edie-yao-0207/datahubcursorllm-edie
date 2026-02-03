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
    a.cm_product_name,
    a.vg_device_id
  from dataprep_firmware.cm_device_daily_metadata as a
  join cohort as b
    on a.org_id = b.org_id
    and a.cm_device_id = b.cm_device_id
  where a.date between ('{{ date_range.start }}') and ('{{ date_range.end }}')
    and ('all' in ({{ vg_firmware }}) or a.last_reported_vg_build in ({{ vg_firmware }}))
    and ('all' in ({{ cm_firmware }}) or a.last_reported_cm_build in ({{ cm_firmware }}))
);

-- Compute thermal mode percentages for each device
create or replace temp view cm_thermal_mode as (
  select
    a.date,
    a.device_id,
    a.cm_device_id,
    b.cm_product_name,
    a.org_id,
    100 * (
      a.total_overheated_duration_ms / a.total_trip_duration_ms
    ) as pct_trip_overheat,
    100 * (
      a.total_safe_mode_duration_ms / a.total_trip_duration_ms
    ) as pct_trip_safemode,
    100 * (
      (
        a.total_safe_mode_duration_ms + a.total_overheated_duration_ms
      ) / total_trip_duration_ms
    ) as pct_trip_thermal_downtime
  from dataprep_safety.cm_device_health_daily as a
  join daily_cohort as b
    on a.date = b.date
    and a.org_id = b.org_id
    and a.cm_device_id = b.cm_device_id
  where a.date between ('{{ date_range.start }}')
    and ('{{ date_range.end }}')
    and cm_last_heartbeat_date >= a.date --remove CMs without heartbeat
);

-- Compute aggregation
create or replace temp view thermal_percentile_agg as (
  select
    date,
    approx_percentile(pct_trip_overheat,.5) as p50_overheat,
    approx_percentile(pct_trip_overheat,.6) as p60_overheat,
    approx_percentile(pct_trip_overheat,.7) as p70_overheat,
    approx_percentile(pct_trip_overheat,.8) as p80_overheat,
    approx_percentile(pct_trip_overheat,.9) as p90_overheat,
    approx_percentile(pct_trip_overheat,.95) as p95_overheat,
    approx_percentile(pct_trip_overheat,.99) as p99_overheat,
    approx_percentile(pct_trip_overheat,.999) as p999_overheat,
    approx_percentile(pct_trip_safemode,.5) as p50_safemode,
    approx_percentile(pct_trip_safemode,.6) as p60_safemode,
    approx_percentile(pct_trip_safemode,.7) as p70_safemode,
    approx_percentile(pct_trip_safemode,.8) as p80_safemode,
    approx_percentile(pct_trip_safemode,.9) as p90_safemode,
    approx_percentile(pct_trip_safemode,.95) as p95_safemode,
    approx_percentile(pct_trip_safemode,.99) as p99_safemode,
    approx_percentile(pct_trip_safemode,.999) as p999_safemode,
    approx_percentile(pct_trip_thermal_downtime, 0.5) as p50_thermal_downtime,
    approx_percentile(pct_trip_thermal_downtime, 0.6) as p60_thermal_downtime,
    approx_percentile(pct_trip_thermal_downtime, 0.7) as p70_thermal_downtime,
    approx_percentile(pct_trip_thermal_downtime, 0.8) as p80_thermal_downtime,
    approx_percentile(pct_trip_thermal_downtime, 0.9) as p90_thermal_downtime,
    approx_percentile(pct_trip_thermal_downtime, 0.95) as p95_thermal_downtime,
    approx_percentile(pct_trip_thermal_downtime, 0.99) as p99_thermal_downtime,
    approx_percentile(pct_trip_thermal_downtime, 0.999) as p999_thermal_downtime
  from cm_thermal_mode
  group by date
);

select *
from thermal_percentile_agg
