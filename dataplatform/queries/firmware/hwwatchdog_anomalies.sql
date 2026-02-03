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
    -- to remove simulated orgs which are test orgs, but not marked as internal orgs
    and org_id not in (
      select
        org_id
      from
        internaldb.simulated_orgs
    )
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

-- Camera Manager hwwatchdog anomalies
create or replace temp view cm_hwwatchdog_anomalies as (
  select
    a.date,
    b.org_id,
    b.cm_device_id as device_id,
    count(*) as num_anomalies
  from
    kinesisstats.osdanomalyevent as a
  join daily_cohort as b
    on a.object_id = b.cm_device_id
  where
    a.date between ('{{ date_range.start }}')
    and ('{{ date_range.end }}')
    and left(value.proto_value.anomaly_event.service_name, charindex('/', value.proto_value.anomaly_event.service_name) - 1) = 'hwwatchdog:firmware.samsaradev.io'
  group by
    a.date,
    b.org_id,
    b.cm_device_id
);

-- Cap to top 20 devices with high anomalies per day
create or replace temp view cm_hwwatchdog_anomalies_capped as (
  select
    date,
    org_id,
    device_id,
    num_anomalies
  from (
    select
      date,
      org_id,
      device_id,
      num_anomalies,
      row_number() over (partition by date order by num_anomalies desc) AS n
    from cm_hwwatchdog_anomalies
    where num_anomalies > 10
  )
  where n <= 20
);

-- Include empty dates for continuity
select * from cm_hwwatchdog_anomalies_capped
union all
  select * from
  (select
    explode(
      sequence(
        cast('{{ date_range.start }}' as date),
        cast('{{ date_range.end }}' as date),
        interval 1 day
      )
    ) as date,
    0 as org_id,
    0 as device_id,
    0 as num_anomalies
  )
  as b
where
  not exists (
    select
      1
    from
      cm_hwwatchdog_anomalies_capped as a
    where
      a.date = b.date
  )
