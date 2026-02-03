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
  join datamodel_core.dim_organizations_settings as org_settings
    on org_settings.date = a.date
    and org_settings.org_id = a.org_id
  where a.date between ('{{ date_range.start }}') and ('{{ date_range.end }}')
    and ('all' in ({{ vg_firmware }}) or a.last_reported_vg_build in ({{ vg_firmware }}))
    and ('all' in ({{ cm_firmware }}) or a.last_reported_cm_build in ({{ cm_firmware }}))
    and org_settings.camera_id_enabled
    and a.org_id not in (select org_id from internaldb.simulated_orgs where org_id is not null)
);

select
  to_date(date) as date,
  count(*) as all_trips,
  count_if(has_album_objectstat) as trips_with_album,
  count_if(has_assigned_driver) as assigned_trips,
  trips_with_album / all_trips as trips_with_album_ratio,
  assigned_trips / trips_with_album as assigned_trips_ratio
from (
  select
    trips.date,
    trips.device_id,
    trips.start_ms,
    first(trips.proto.driver_face_id.driver_id) <> 0 has_assigned_driver,
    first(objectstats.time) is not null as has_album_objectstat
  from
    trips2db_shards.trips as trips
    join daily_cohort on daily_cohort.vg_device_id = trips.device_id and daily_cohort.date = trips.date
    left join kinesisstats.osddashcamdriveralbum as objectstats on true
      and objectstats.date = trips.date
      and objectstats.object_id = daily_cohort.cm_device_id
      and objectstats.time between trips.start_ms and trips.end_ms
  where true
    and trips.date between "{{ date_range.start }}" and "{{ date_range.end }}"
    -- drop trips that start close to midnight because we're not checking
    -- for driver album on the next day to speed up the query
    and trips.date = from_unixtime(trips.end_ms / 1000, "yyyy-MM-dd")
  group by
    trips.date,
    trips.device_id,
    trips.start_ms
  )
group by
  date
