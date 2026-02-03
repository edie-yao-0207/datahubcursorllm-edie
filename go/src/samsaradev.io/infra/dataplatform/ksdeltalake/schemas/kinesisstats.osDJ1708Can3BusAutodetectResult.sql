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
    `j1708_can3_autodetect_metadata`: STRUCT<
      `can3_autodetect_duration_ms`: BIGINT,
      `j1708_autodetect_duration_ms`: BIGINT,
      `bus_autodetect_method`: INT,
      `vehicle_active_source`: INT,
      `bus_not_present_count`: BIGINT,
      `bus_not_present_count_before_cached`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
