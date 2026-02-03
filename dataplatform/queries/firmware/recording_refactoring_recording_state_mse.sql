create or replace temp view cohort as (
	select
    org_id,
    cm_device_id,
    cm_product_name,
    cm_rollout_stage_id,
    last_reported_cm_build as latest_cm_firmware,
    last_reported_vg_build as latest_vg_firmware
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
  select a.date, b.*
  from dataprep_firmware.cm_device_daily_metadata a
  inner join cohort b
    on a.cm_device_id = b.cm_device_id
  inner join dataprep_firmware.cm_device_daily_metadata prev_day
    on a.cm_device_id = prev_day.cm_device_id
    and prev_day.date = date_add(a.date, -1) -- ensuring the join is based on the previous day's data
  where a.date between ('{{ date_range.start }}') and ('{{ date_range.end }}')
    and ( 'all' in ({{ vg_firmware }}) or a.last_reported_vg_build in ({{ vg_firmware }}) )
    and ( 'all' in ({{ cm_firmware }}) or a.last_reported_cm_build in ({{ cm_firmware }}) )
    and ( 'all' in ({{ vg_firmware }}) or prev_day.last_reported_vg_build in ({{ vg_firmware }}) ) -- ensuring the previous day's VG firmware is in the selection
    and ( 'all' in ({{ cm_firmware }}) or prev_day.last_reported_cm_build in ({{ cm_firmware }}) ) -- ensuring the previous day's CM firmware is in the selection
);


with weighted_mse as (
  select
    a.date,
    a.device_id,
    sum((is_actual_recording - is_target_recording) * (is_actual_recording - is_target_recording) * (end_ms - start_ms))
      / sum(end_ms - start_ms) as weighted_mse,
    case when dayofweek(a.date) in (1, 7) then 'Weekend' else 'Weekday' end as day_type
  from
    dataprep_firmware.cm_actual_and_target_recording_intervals_daily a
    join daily_cohort b on a.device_id = b.cm_device_id and a.date = b.date
  group by
    a.date,
    a.device_id
)
select
  date,
  percentile_approx(weighted_mse, 0.70) as weighted_mse_p70,
  percentile_approx(weighted_mse, 0.80) as weighted_mse_p80,
  percentile_approx(weighted_mse, 0.90) as weighted_mse_p90,
  percentile_approx(weighted_mse, 0.95) as weighted_mse_p95,
  percentile_approx(weighted_mse, 0.99) as weighted_mse_p99
from
  weighted_mse
where ( 'all' in ({{ day_type }}) or day_type in ({{ day_type }}) )
group by date
