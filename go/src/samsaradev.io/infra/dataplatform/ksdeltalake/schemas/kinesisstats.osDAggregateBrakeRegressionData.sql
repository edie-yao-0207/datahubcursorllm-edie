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
    `aggregate_brake_regression_data`: STRUCT<
      `start_time_ms`: DECIMAL(20, 0),
      `count`: DECIMAL(20, 0),
      `sum_pressure_bar`: DOUBLE,
      `sum_braking_effort_percent`: DOUBLE,
      `sum_pressure_bar_squared`: DOUBLE,
      `sum_braking_effort_percent_squared`: DOUBLE,
      `sum_pressure_bar_times_braking_effort_percent`: DOUBLE,
      `count_below_braking_threshold`: DECIMAL(20, 0),
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
