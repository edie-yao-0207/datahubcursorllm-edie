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
  >
>,
`_filename` STRING,
`_sort_key` STRING
