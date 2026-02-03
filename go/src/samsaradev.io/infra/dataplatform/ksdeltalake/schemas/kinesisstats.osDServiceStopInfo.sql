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
    `service_stop_info`: STRUCT<
      `service_name`: STRING,
      `stop_time_ms`: BIGINT,
      `number_of_kill_signals_sent`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
