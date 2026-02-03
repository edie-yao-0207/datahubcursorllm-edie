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
    a.cm_product_name
  from dataprep_firmware.cm_device_daily_metadata as a
  join cohort as b
    on a.org_id = b.org_id
    and a.cm_device_id = b.cm_device_id
  where a.date between ('{{ date_range.start }}') and ('{{ date_range.end }}')
    and ('all' in ({{ vg_firmware }}) or a.last_reported_vg_build in ({{ vg_firmware }}))
    and ('all' in ({{ cm_firmware }}) or a.last_reported_cm_build in ({{ cm_firmware }}))
);

-- create factor to correct primary overheat based on short overheat intervals
-- during short overheat intervals the expected avg fps is 22.5
-- which is the average bewteen 30fps and 15fps
create or replace temp view primary_overheat_correct_factor as (
  select
    date_start,
    cm_device_id,
    case
      when count(*) = 0 then 1
      else 15 * count(*) / (
        count_if((end_ms - start_ms) < 60000) * 22.5 + count_if((end_ms - start_ms) >= 60000) * 15
      )
    end as correct_factor
  from dataprep_safety.cm_overheated_intervals
  where
    date_start between ('{{ date_range.start }}')
    and ('{{ date_range.end }}')
    and is_overheated = 1
  group by
    1,
    2
);

create or replace temp view cm_video_fps_daily_corrected as (
  select
    a.*,
    average_framerate * b.correct_factor as corrected_fps -- only to be used for overheat primary
  from dataprep_safety.cm_video_fps_daily as a
  join primary_overheat_correct_factor as b
    on a.date = b.date_start
    and a.cm_device_id = b.cm_device_id
);

-- Create an intermediate view to calculate percentiles for each stream_source and thermal_state combination
create or replace temp view fps_percentiles as (
  select
    a.date,
    stream_source,
    thermal_state,
    b.cm_product_name,
    percentile(case when stream_source = 'primary' and thermal_state = 'overheated' then corrected_fps else average_framerate end, 0.01) as p001,
    percentile(case when stream_source = 'primary' and thermal_state = 'overheated' then corrected_fps else average_framerate end, 0.1) as p01,
    percentile(case when stream_source = 'primary' and thermal_state = 'overheated' then corrected_fps else average_framerate end, 0.05) as p05,
    percentile(case when stream_source = 'primary' and thermal_state = 'overheated' then corrected_fps else average_framerate end, 0.1) as p10,
    percentile(case when stream_source = 'primary' and thermal_state = 'overheated' then corrected_fps else average_framerate end, 0.25) as p25,
    percentile(case when stream_source = 'primary' and thermal_state = 'overheated' then corrected_fps else average_framerate end, 0.5) as p50,
    percentile(case when stream_source = 'primary' and thermal_state = 'overheated' then corrected_fps else average_framerate end, 0.75) as p75,
    percentile(case when stream_source = 'primary' and thermal_state = 'overheated' then corrected_fps else average_framerate end, 0.99) as p99
  from cm_video_fps_daily_corrected as a
  join daily_cohort as b
    on a.date = b.date
    and a.org_id = b.org_id
    and a.cm_device_id = b.cm_device_id
  where a.date between ('{{ date_range.start }}')
    and ('{{ date_range.end }}')
  group by a.date, stream_source, thermal_state, b.cm_product_name
);

select
  date,
  
    -- Primary normal
  max(case when stream_source = 'primary' and thermal_state = 'normal' then p001 end) as primary_normal_p001,
  max(case when stream_source = 'primary' and thermal_state = 'normal' then p01 end) as primary_normal_p01,
  max(case when stream_source = 'primary' and thermal_state = 'normal' then p05 end) as primary_normal_p05,
  max(case when stream_source = 'primary' and thermal_state = 'normal' then p10 end) as primary_normal_p10,
  max(case when stream_source = 'primary' and thermal_state = 'normal' then p25 end) as primary_normal_p25,
  max(case when stream_source = 'primary' and thermal_state = 'normal' then p50 end) as primary_normal_p50,
  max(case when stream_source = 'primary' and thermal_state = 'normal' then p75 end) as primary_normal_p75,
  max(case when stream_source = 'primary' and thermal_state = 'normal' then p99 end) as primary_normal_p99,
  
  -- Primary overheated
  max(case when stream_source = 'primary' and thermal_state = 'overheated' then p001 end) as primary_overheated_p001,
  max(case when stream_source = 'primary' and thermal_state = 'overheated' then p01 end) as primary_overheated_p01,
  max(case when stream_source = 'primary' and thermal_state = 'overheated' then p05 end) as primary_overheated_p05,
  max(case when stream_source = 'primary' and thermal_state = 'overheated' then p10 end) as primary_overheated_p10,
  max(case when stream_source = 'primary' and thermal_state = 'overheated' then p25 end) as primary_overheated_p25,
  max(case when stream_source = 'primary' and thermal_state = 'overheated' then p50 end) as primary_overheated_p50,
  max(case when stream_source = 'primary' and thermal_state = 'overheated' then p75 end) as primary_overheated_p75,
  max(case when stream_source = 'primary' and thermal_state = 'overheated' then p99 end) as primary_overheated_p99,
  
  -- Primary safe_mode
  max(case when stream_source = 'primary' and thermal_state = 'safe_mode' then p001 end) as primary_safe_mode_p001,
  max(case when stream_source = 'primary' and thermal_state = 'safe_mode' then p01 end) as primary_safe_mode_p01,
  max(case when stream_source = 'primary' and thermal_state = 'safe_mode' then p05 end) as primary_safe_mode_p05,
  max(case when stream_source = 'primary' and thermal_state = 'safe_mode' then p10 end) as primary_safe_mode_p10,
  max(case when stream_source = 'primary' and thermal_state = 'safe_mode' then p25 end) as primary_safe_mode_p25,
  max(case when stream_source = 'primary' and thermal_state = 'safe_mode' then p50 end) as primary_safe_mode_p50,
  max(case when stream_source = 'primary' and thermal_state = 'safe_mode' then p75 end) as primary_safe_mode_p75,
  max(case when stream_source = 'primary' and thermal_state = 'safe_mode' then p99 end) as primary_safe_mode_p99,
  
  -- Primary low res normal
  max(case when stream_source = 'primary_low_res' and thermal_state = 'normal' then p001 end) as primary_low_res_normal_p001,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'normal' then p01 end) as primary_low_res_normal_p01,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'normal' then p05 end) as primary_low_res_normal_p05,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'normal' then p10 end) as primary_low_res_normal_p10,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'normal' then p25 end) as primary_low_res_normal_p25,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'normal' then p50 end) as primary_low_res_normal_p50,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'normal' then p75 end) as primary_low_res_normal_p75,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'normal' then p99 end) as primary_low_res_normal_p99,
  
  -- Primary low res overheated
  max(case when stream_source = 'primary_low_res' and thermal_state = 'overheated' then p001 end) as primary_low_res_overheated_p001,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'overheated' then p01 end) as primary_low_res_overheated_p01,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'overheated' then p05 end) as primary_low_res_overheated_p05,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'overheated' then p10 end) as primary_low_res_overheated_p10,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'overheated' then p25 end) as primary_low_res_overheated_p25,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'overheated' then p50 end) as primary_low_res_overheated_p50,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'overheated' then p75 end) as primary_low_res_overheated_p75,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'overheated' then p99 end) as primary_low_res_overheated_p99,
  
  -- Primary low res safe_mode
  max(case when stream_source = 'primary_low_res' and thermal_state = 'safe_mode' then p001 end) as primary_low_res_safe_mode_p001,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'safe_mode' then p01 end) as primary_low_res_safe_mode_p01,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'safe_mode' then p05 end) as primary_low_res_safe_mode_p05,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'safe_mode' then p10 end) as primary_low_res_safe_mode_p10,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'safe_mode' then p25 end) as primary_low_res_safe_mode_p25,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'safe_mode' then p50 end) as primary_low_res_safe_mode_p50,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'safe_mode' then p75 end) as primary_low_res_safe_mode_p75,
  max(case when stream_source = 'primary_low_res' and thermal_state = 'safe_mode' then p99 end) as primary_low_res_safe_mode_p99,
  
  -- Secondary normal
  max(case when stream_source = 'secondary' and thermal_state = 'normal' then p001 end) as secondary_normal_p001,
  max(case when stream_source = 'secondary' and thermal_state = 'normal' then p01 end) as secondary_normal_p01,
  max(case when stream_source = 'secondary' and thermal_state = 'normal' then p05 end) as secondary_normal_p05,
  max(case when stream_source = 'secondary' and thermal_state = 'normal' then p10 end) as secondary_normal_p10,
  max(case when stream_source = 'secondary' and thermal_state = 'normal' then p25 end) as secondary_normal_p25,
  max(case when stream_source = 'secondary' and thermal_state = 'normal' then p50 end) as secondary_normal_p50,
  max(case when stream_source = 'secondary' and thermal_state = 'normal' then p75 end) as secondary_normal_p75,
  max(case when stream_source = 'secondary' and thermal_state = 'normal' then p99 end) as secondary_normal_p99,
  
  -- Secondary overheated
  max(case when stream_source = 'secondary' and thermal_state = 'overheated' then p001 end) as secondary_overheated_p001,
  max(case when stream_source = 'secondary' and thermal_state = 'overheated' then p01 end) as secondary_overheated_p01,
  max(case when stream_source = 'secondary' and thermal_state = 'overheated' then p05 end) as secondary_overheated_p05,
  max(case when stream_source = 'secondary' and thermal_state = 'overheated' then p10 end) as secondary_overheated_p10,
  max(case when stream_source = 'secondary' and thermal_state = 'overheated' then p25 end) as secondary_overheated_p25,
  max(case when stream_source = 'secondary' and thermal_state = 'overheated' then p50 end) as secondary_overheated_p50,
  max(case when stream_source = 'secondary' and thermal_state = 'overheated' then p75 end) as secondary_overheated_p75,
  max(case when stream_source = 'secondary' and thermal_state = 'overheated' then p99 end) as secondary_overheated_p99,
  
  -- Secondary safe_mode
  max(case when stream_source = 'secondary' and thermal_state = 'safe_mode' then p001 end) as secondary_safe_mode_p001,
  max(case when stream_source = 'secondary' and thermal_state = 'safe_mode' then p01 end) as secondary_safe_mode_p01,
  max(case when stream_source = 'secondary' and thermal_state = 'safe_mode' then p05 end) as secondary_safe_mode_p05,
  max(case when stream_source = 'secondary' and thermal_state = 'safe_mode' then p10 end) as secondary_safe_mode_p10,
  max(case when stream_source = 'secondary' and thermal_state = 'safe_mode' then p25 end) as secondary_safe_mode_p25,
  max(case when stream_source = 'secondary' and thermal_state = 'safe_mode' then p50 end) as secondary_safe_mode_p50,
  max(case when stream_source = 'secondary' and thermal_state = 'safe_mode' then p75 end) as secondary_safe_mode_p75,
  max(case when stream_source = 'secondary' and thermal_state = 'safe_mode' then p99 end) as secondary_safe_mode_p99,
  
  -- Secondary low res normal
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'normal' then p001 end) as secondary_low_res_normal_p001,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'normal' then p01 end) as secondary_low_res_normal_p01,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'normal' then p05 end) as secondary_low_res_normal_p05,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'normal' then p10 end) as secondary_low_res_normal_p10,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'normal' then p25 end) as secondary_low_res_normal_p25,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'normal' then p50 end) as secondary_low_res_normal_p50,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'normal' then p75 end) as secondary_low_res_normal_p75,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'normal' then p99 end) as secondary_low_res_normal_p99,
  
  -- Secondary low res overheated
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'overheated' then p001 end) as secondary_low_res_overheated_p001,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'overheated' then p01 end) as secondary_low_res_overheated_p01,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'overheated' then p05 end) as secondary_low_res_overheated_p05,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'overheated' then p10 end) as secondary_low_res_overheated_p10,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'overheated' then p25 end) as secondary_low_res_overheated_p25,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'overheated' then p50 end) as secondary_low_res_overheated_p50,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'overheated' then p75 end) as secondary_low_res_overheated_p75,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'overheated' then p99 end) as secondary_low_res_overheated_p99,
  
  -- Secondary low res safe_mode
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'safe_mode' then p001 end) as secondary_low_res_safe_mode_p001,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'safe_mode' then p01 end) as secondary_low_res_safe_mode_p01,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'safe_mode' then p05 end) as secondary_low_res_safe_mode_p05,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'safe_mode' then p10 end) as secondary_low_res_safe_mode_p10,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'safe_mode' then p25 end) as secondary_low_res_safe_mode_p25,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'safe_mode' then p50 end) as secondary_low_res_safe_mode_p50,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'safe_mode' then p75 end) as secondary_low_res_safe_mode_p75,
  max(case when stream_source = 'secondary_low_res' and thermal_state = 'safe_mode' then p99 end) as secondary_low_res_safe_mode_p99
from fps_percentiles
group by date

