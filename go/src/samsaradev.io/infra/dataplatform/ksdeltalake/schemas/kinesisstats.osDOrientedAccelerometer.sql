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
    `oriented_harsh_event_metadata`: STRUCT<
      `did_trigger_event`: BOOLEAN,
      `trigger_reason`: INT,
      `min_accel_x`: DOUBLE,
      `max_accel_x`: DOUBLE,
      `oriented_harsh_event_config`: STRUCT<
        `enable`: BOOLEAN,
        `harsh_accel_x_threshold_gs`: FLOAT,
        `harsh_brake_x_threshold_gs`: FLOAT,
        `harsh_turn_x_threshold_gs`: FLOAT,
        `accel_window_size_ms`: BIGINT,
        `median_filter_span_ms`: BIGINT,
        `ewma_filter_span_ms`: BIGINT,
        `debug_mode`: BOOLEAN,
        `disable`: BOOLEAN,
        `debug_mode_past_12_1`: BOOLEAN
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
