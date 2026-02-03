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
    `industrial_protocol_polling_duration`: STRUCT<
      `protocol`: INT,
      `duration_ms_50_percentile`: BIGINT,
      `duration_ms_90_percentile`: BIGINT,
      `duration_ms_99_percentile`: BIGINT,
      `duration_count`: BIGINT,
      `num_configured_pins`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
