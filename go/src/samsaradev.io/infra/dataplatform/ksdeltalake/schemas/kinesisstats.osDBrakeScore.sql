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
    `brake_score`: STRUCT<
      `braking_effort_percent_score`: DOUBLE,
      `metadata`: STRUCT<
        `measured_pressure_bar`: DOUBLE,
        `version`: DECIMAL(20, 0),
        `margin_of_error`: DOUBLE,
        `confidence_interval`: DOUBLE,
        `regression_slope`: DOUBLE,
        `regression_brake_performance_intercept`: DOUBLE
      >,
      `start_time_ms`: DECIMAL(20, 0),
      `event_count`: DECIMAL(20, 0),
      `event_below_threshold_count`: DECIMAL(20, 0),
      `weighted_braking_effort_percent_score`: DOUBLE,
      `weighted_metadata`: STRUCT<
        `measured_pressure_bar`: DOUBLE,
        `version`: DECIMAL(20, 0),
        `margin_of_error`: DOUBLE,
        `confidence_interval`: DOUBLE,
        `regression_slope`: DOUBLE,
        `regression_brake_performance_intercept`: DOUBLE
      >,
      `check_ebs_metadata`: STRUCT<
        `active`: BOOLEAN,
        `last_triggered_at_ms`: DECIMAL(20, 0),
        `trigger_count`: BIGINT,
        `threshold_pressure_bar`: DOUBLE
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
