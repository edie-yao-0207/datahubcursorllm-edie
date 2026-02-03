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
    `vrpc_client_metrics`: STRUCT<
      `connection_state`: INT,
      `successful_calls_this_boot`: BIGINT,
      `calls_this_boot`: BIGINT,
      `longest_disconnected_time_this_boot_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
