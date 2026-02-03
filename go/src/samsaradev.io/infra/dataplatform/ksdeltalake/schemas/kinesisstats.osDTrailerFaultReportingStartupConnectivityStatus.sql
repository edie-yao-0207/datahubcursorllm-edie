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
    `trailer_fault_reporting_startup_connectivity_status`: STRUCT<
      `cable_type`: INT,
      `startup_connectivity_type`: INT,
      `start_signal_detection_period_ms`: BIGINT,
      `start_signal_debounce_ms`: BIGINT,
      `sample_period_ms`: BIGINT,
      `start_time_after_power_on_ms`: BIGINT,
      `end_time_after_power_on_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
