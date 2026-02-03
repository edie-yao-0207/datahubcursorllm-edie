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
    `gps_time_to_first_fix_info`: STRUCT<
      `time_since_last_fix_ms`: BIGINT,
      `time_to_first_fix_type`: INT,
      `nordic_system_state`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
