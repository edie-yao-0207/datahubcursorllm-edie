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

create or replace temporary view trip_state_transitions as (
  select
    to_date(trip_states.date) as date,
    org_id,
    object_id as vg_device_id,
    daily_cohort.cm_device_id as cm_device_id,
    time,
    -- Find the time value of the next objectstat for this device and default to end of the date range if no such objectstat exists
    coalesce(
      lead(time) over (
        partition by object_id
        order by
          trip_states.date,
          time asc
      ),
      to_unix_timestamp("{{ date_range.end }}") * 1000
    ) as next_time,
    -- Trip state objectstats are emitted every 10 minutes. Assume there is a databreak and treat the trip as stopped if there is a gap larger than 11 minutes.
    if(
      next_time - time <= 11 * 60 * 1000,
      value.int_value,
      0
    ) as trip_state,
    -- Find the trip state of the previous objectstat for this device and default to the current state if no such objectstat exists.
    -- This results in any trip that is already in progress at the start of the date range to be ignored because there will be no state transition.
    -- Alternatively, we can fall back to zero so that any trip that is in progress will "start" at the time of current objectstat.
    coalesce(
      lag(value.int_value, 1) over (
        partition by object_id
        order by
          trip_states.date,
          time asc
      ),
      trip_state
    ) as prev_trip_state,
    (
      trip_state = 1
      and prev_trip_state = 0
    ) as is_trip_start,
    (
      trip_state = 0
      and prev_trip_state = 1
    ) as is_trip_end
  from
    kinesisstats.osdfirmwaretripstate as trip_states
    join daily_cohort on object_id = daily_cohort.vg_device_id and trip_states.date = daily_cohort.date
);

create or replace temporary view trips as (
  select
    date,
    org_id,
    vg_device_id,
    cm_device_id,
    time as trip_start_ms,
    next_time as trip_end_ms
  from
    (
      select
        date,
        org_id,
        time,
        vg_device_id,
        cm_device_id,
        is_trip_start,
        -- Assume the last emitted trip state lasted until the end of the date range
        coalesce(
          lead(time) over (
            partition by vg_device_id
            order by
              date,
              time asc
          ),
          to_unix_timestamp("{{ date_range.end }}") * 1000
        ) as next_time,
        is_trip_end
      from
        trip_state_transitions
      where
        is_trip_start
        or is_trip_end
    )
  where
    is_trip_start
);

select
  org_id,
  vg_device_id,
  cm_device_id,
  count(*) as all_trips,
  count_if(has_album_objectstat) as trips_with_album,
  trips_with_album / all_trips as trips_with_album_ratio
from (
  select
    trips.date,
    trips.vg_device_id,
    trips.trip_start_ms,
    first(trips.org_id) as org_id,
    first(trips.cm_device_id) as cm_device_id,
    first(objectstats.time) is not null as has_album_objectstat
  from
    trips
  left join kinesisstats.osddashcamdriveralbum as objectstats on true
    and objectstats.date = trips.date
    and objectstats.object_id = trips.cm_device_id
    and objectstats.time between trips.trip_start_ms and trips.trip_end_ms
  where true
    and trips.date between "{{ date_range.start }}" and "{{ date_range.end }}"
    -- drop trips that start close to midnight because we're not checking
    -- for driver album on the next day to speed up the query
    and from_unixtime(trips.trip_start_ms / 1000, "HH:mm") < "23:55"
  group by
    trips.date,
    trips.vg_device_id,
    trips.trip_start_ms
  )
group by
  org_id,
  vg_device_id,
  cm_device_id
order by
  trips_with_album_ratio asc,
  all_trips desc
