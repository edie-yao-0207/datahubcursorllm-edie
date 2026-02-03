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
    `qcs603_emergency_downloader_mode_recovery_info`: STRUCT<
      `duration_ms`: BIGINT,
      `method`: INT,
      `successful`: BOOLEAN,
      `used_golang_sahara_reset_implementation`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
