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

create or replace temp view filtered_devices as (
  select
    a.date,
    a.org_id,
    a.cm_device_id as device_id
  from
    daily_cohort as a
    join dataprep_firmware.speeding_daily_active_devices as b
      on a.date = b.date
      and a.cm_device_id = b.device_id
      and a.org_id = b.org_id
);

create or replace temp view filtered_query as (
  select
    a.date,
    case -- https://github.com/samsara-dev/firmware/blob/master/samsara/go/src/third_party_modified/samsaradev.io/hubproto/maptileproto/vehicle_type_tags.proto#L7
      when a.speed_limit_vehicle_type = 0 then "N/A"
      when a.speed_limit_vehicle_type = 1 then "DEFAULT"
      when a.speed_limit_vehicle_type = 2 then "BUS"
      when a.speed_limit_vehicle_type = 23 then "GOODS_VEHICLE_NO_MAX_LADEN"
      when a.speed_limit_vehicle_type = 24 then "GOODS_VEHICLE_2_T"
      when a.speed_limit_vehicle_type = 25 then "GOODS_VEHICLE_3_5_T"
      when a.speed_limit_vehicle_type = 26 then "GOODS_VEHICLE_7_5_T"
      when a.speed_limit_vehicle_type = 27 then "GOODS_VEHICLE_12_T"
      when a.speed_limit_vehicle_type = 28 then "GOODS_VEHICLE_MORE_THAN_12_T"
      when a.speed_limit_vehicle_type = 29 then "GOODS_VEHICLE_10000_LBS"
      when a.speed_limit_vehicle_type = 30 then "GOODS_VEHICLE_26000_LBS"
      when a.speed_limit_vehicle_type = 31 then "GOODS_VEHICLE_MORE_THAN_26000_LBS"
      when a.speed_limit_vehicle_type = 32 then "PASSENGER"
      else a.speed_limit_vehicle_type
    end as speed_limit_vehicle_type_string,
    sum(a.count) as count
  from dataprep_firmware.speeding_speed_limit_count as a
  join filtered_devices as b
    on a.date = b.date
    and a.org_id = b.org_id
    and a.device_id = b.device_id
  group by
    a.date,
    a.speed_limit_vehicle_type
);

select
  date,
  speed_limit_vehicle_type_string,
  count
from filtered_query
