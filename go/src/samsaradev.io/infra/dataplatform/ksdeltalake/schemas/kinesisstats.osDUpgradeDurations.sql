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
    `upgrade_durations`: STRUCT<
      `build`: STRING,
      `time_from_first_push_to_reboot_ms`: DECIMAL(20, 0),
      `delta_download_duration_ms`: DECIMAL(20, 0),
      `delta_apply_duration_ms`: DECIMAL(20, 0),
      `full_script_download_duration_ms`: DECIMAL(20, 0),
      `successfully_used_delta`: BOOLEAN,
      `attempted_to_use_delta`: BOOLEAN,
      `upgrade_script_duration_ms`: DECIMAL(20, 0),
      `wait_duration_ms`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
