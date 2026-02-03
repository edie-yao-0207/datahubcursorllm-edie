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

with dc_serials as (
  select distinct
    a.date,
    a.org_id,
    dd.serial as cm_serial
  from daily_cohort a
  join datamodel_core.dim_devices dd
    on dd.date = a.date
   and dd.org_id = a.org_id
   and dd.device_id = a.cm_device_id
)
select
    d.date,
    c.product,
    count(distinct d.cm_serial) as serial_count,
    count(*) as total_count,
    count_if(disconnected_seconds > 1000) as count_over_1000_seconds
from hardware_analytics.cm_vg_disconnections_w_signtatures d
join hardware_analytics.cm_health_daily_with_signatures c
    on c.date = d.date
   and c.cm_serial = d.cm_serial
where string(vg_anomalies) like '%Resetting USB%'
  and exists (
        select 1
        from dc_serials s
        where s.date = d.date
          and s.cm_serial = c.cm_serial
      )
group by 1, 2
order by d.date asc, c.product asc;
