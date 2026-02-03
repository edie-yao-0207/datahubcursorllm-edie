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
    a.cm_device_id as device_id
  from dataprep_firmware.cm_device_daily_metadata as a
  join cohort as b
    on a.org_id = b.org_id
    and a.cm_device_id = b.cm_device_id
  join dataprep_firmware.low_bridge_strike_daily_active_devices as c
    on a.date = c.date
    and a.cm_device_id = c.device_id
    and a.org_id = c.org_id
  where a.date between ('{{ date_range.start }}') and ('{{ date_range.end }}')
    and ('all' in ({{ vg_firmware }}) or a.last_reported_vg_build in ({{ vg_firmware }}))
    and ('all' in ({{ cm_firmware }}) or a.last_reported_cm_build in ({{ cm_firmware }}))
);

select
  b.date,
  mean(distance_to_bridge_node_meters) as avg,
  percentile_approx(distance_to_bridge_node_meters, 0.01) as p01,
  percentile_approx(distance_to_bridge_node_meters, 0.1) as p10,
  percentile_approx(distance_to_bridge_node_meters, 0.25) as p25,
  percentile_approx(distance_to_bridge_node_meters, 0.5) as median,
  percentile_approx(distance_to_bridge_node_meters, 0.75) as p75,
  percentile_approx(distance_to_bridge_node_meters, 0.90) as p90,
  percentile_approx(distance_to_bridge_node_meters, 0.99) as p99
from dataprep_firmware.low_bridge_strike_warning_events as a
right join daily_cohort as b
  on a.date = b.date
  and a.org_id = b.org_id
  and a.device_id = b.device_id
group by
  b.date
order by
  b.date desc
