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


select
  a.date,
  avg(a.cm_uptime) as avg,
  percentile_approx(a.cm_uptime, 0.5) as median,
  percentile_approx(a.cm_uptime, 0.4) as p40,
  percentile_approx(a.cm_uptime, 0.3) as p30,
  percentile_approx(a.cm_uptime, 0.2) as p20,
  percentile_approx(a.cm_uptime, 0.1) as p10,
  percentile_approx(a.cm_uptime, 0.09) as p09,
  percentile_approx(a.cm_uptime, 0.08) as p08,
  percentile_approx(a.cm_uptime, 0.07) as p07,
  percentile_approx(a.cm_uptime, 0.06) as p06,
  percentile_approx(a.cm_uptime, 0.05) as p05,
  percentile_approx(a.cm_uptime, 0.04) as p04,
  percentile_approx(a.cm_uptime, 0.03) as p03,
  percentile_approx(a.cm_uptime, 0.02) as p02,
  percentile_approx(a.cm_uptime, 0.01) as p01,
  percentile_approx(a.cm_uptime, 0.001) as p001
from dataprep_firmware.cm_final_uptime as a
join daily_cohort as b
  on a.date = b.date
  and a.org_id = b.org_id
  and a.device_id = b.cm_device_id
where a.date between ('{{ date_range.start }}') and ('{{ date_range.end }}')
group by a.date
