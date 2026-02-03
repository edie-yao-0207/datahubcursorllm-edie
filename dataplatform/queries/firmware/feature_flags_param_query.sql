(
  select feature_flag_name_value
  from dataprep_firmware.all_feature_flags
)
union
(
  select 'all' as feature_flag_name_value
)
order by feature_flag_name_value desc
