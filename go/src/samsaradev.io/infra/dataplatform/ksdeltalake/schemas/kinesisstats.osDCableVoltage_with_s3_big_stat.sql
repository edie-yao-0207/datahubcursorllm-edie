`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`_s3_big_stat_file_created_at_ms` BIGINT,
`s3_proto_value` STRUCT<
  `external_voltage_samples_batch`: ARRAY<
    STRUCT<
      `offset_ms`: DECIMAL(20, 0),
      `voltage_mv`: DECIMAL(20, 0),
      `is_live`: BOOLEAN
    >
  >
>,
`_synced_at` TIMESTAMP,
`value` STRUCT<
  `date`: STRING,
  `time`: BIGINT,
  `is_start`: BOOLEAN,
  `is_end`: BOOLEAN,
  `is_databreak`: BOOLEAN,
  `int_value`: BIGINT,
  `double_value`: DOUBLE,
  `proto_value`: STRUCT<
    `analog_input_millivolts`: STRUCT<
      `duration_ms`: DECIMAL(20, 0),
      `current_value`: BIGINT,
      `max_value`: BIGINT,
      `min_value`: BIGINT,
      `is_engine_state_triggered_batch`: BOOLEAN,
      `batch_window_duration_ms`: DECIMAL(20, 0),
      `min_value_batch`: BIGINT,
      `max_value_batch`: BIGINT
    >
  >,
  `received_delta_seconds`: BIGINT
>
