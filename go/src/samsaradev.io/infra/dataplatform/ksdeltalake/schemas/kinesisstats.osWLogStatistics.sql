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
    `widget_log_statistics`: STRUCT<
      `gateway_id`: DECIMAL(20, 0),
      `statistics_window_size_ms`: BIGINT,
      `num_logs`: BIGINT,
      `includes_backlogged_events`: BOOLEAN,
      `time_since_event_p50_ms`: BIGINT,
      `time_since_event_p75_ms`: BIGINT,
      `time_since_event_p95_ms`: BIGINT,
      `time_since_event_p99_ms`: BIGINT,
      `time_since_event_avg_ms`: BIGINT,
      `time_since_event_std_dev_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
