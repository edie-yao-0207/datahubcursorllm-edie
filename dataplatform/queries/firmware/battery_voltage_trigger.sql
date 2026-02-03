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
    a.vg_device_id,
    a.last_reported_cm_build
  from dataprep_firmware.cm_device_daily_metadata as a
  join cohort as b
    on a.org_id = b.org_id
    and a.cm_device_id = b.cm_device_id
  join datamodel_core.dim_organizations_settings as org_settings
    on org_settings.date = a.date
    and org_settings.org_id = a.org_id
  join datamodel_core.lifetime_device_activity as activity
    on activity.date = a.date
    and activity.device_id = a.cm_device_id
    and activity.l1 = 1
  where a.date between ('{{ date_range.start }}') and ('{{ date_range.end }}')
    and ('all' in ({{ vg_firmware }}) or a.last_reported_vg_build in ({{ vg_firmware }}))
    and ('all' in ({{ cm_firmware }}) or a.last_reported_cm_build in ({{ cm_firmware }}))
    and org_settings.camera_id_enabled
);

create or replace temporary view devices_with_config as (
select
  daily_cohort.date,
  daily_cohort.cm_device_id,
  first(daily_cohort.last_reported_cm_build) as last_reported_cm_build,
  max_by(coalesce(configs.s3_proto_value.reported_device_config.device_config.vision.secondary_inference.conditional_hand_face_detector_enabled, 0), time) <> 2 as conditional_hand_face_detector_enabled,
  conditional_hand_face_detector_enabled and coalesce(first(daily_cohort.last_reported_cm_build), "") >= "2024-03-13_cm-31.0.8" as thermal_optimization_active,
  max_by(coalesce(array_size(s3_proto_value.reported_device_config.device_config.cm3x_thermal_limiter_config.sensors), 0), time) <> 2 as safe_mode_voltage_trigger_enabled
from
  daily_cohort
  left join s3bigstats.osdreporteddeviceconfig_raw configs
    on configs.object_id = daily_cohort.cm_device_id
    and configs.date >= "2024-03-18"
    and configs.date <= daily_cohort.date
where
  last_reported_cm_build is not null
group by
  daily_cohort.date,
  daily_cohort.cm_device_id
);

select
  to_date(date) as date,
  last_reported_cm_build,
  thermal_optimization_active,
  safe_mode_voltage_trigger_enabled,
  count(*) as count
from
  devices_with_config
group by
  date,
  last_reported_cm_build,
  thermal_optimization_active,
  safe_mode_voltage_trigger_enabled
