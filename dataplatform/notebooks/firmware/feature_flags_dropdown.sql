create or replace temp view exploded as (
  select
    date,
    org_id,
    gateway_id,
    explode(enabled_feature_flag_values) as value
  from dataprep_firmware.gateway_feature_flag_states_daily
);

create or replace table dataprep_firmware.all_feature_flags as (
  select
    concat(value.key, ":", value.value) as feature_flag_name_value
  from exploded
  group by concat(value.key, ":", value.value)
);
