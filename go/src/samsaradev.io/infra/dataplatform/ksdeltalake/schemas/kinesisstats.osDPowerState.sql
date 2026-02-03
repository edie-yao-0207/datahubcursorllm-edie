`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`value` STRUCT<
  `date`: STRING,
  `time`: BIGINT,
  `received_delta_seconds`: BIGINT,
  `is_start`: BOOLEAN,
  `is_end`: BOOLEAN,
  `is_databreak`: BOOLEAN,
  `int_value`: BIGINT,
  `double_value`: DOUBLE,
  `proto_value`: STRUCT<
    `power_state_info`: STRUCT<
      `last_power_state`: BIGINT,
      `current_power_state`: BIGINT,
      `change_reason`: INT,
      `duration_reason`: INT,
      `override_power_duration_active`: BOOLEAN,
      `sleep_duration_seconds`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
