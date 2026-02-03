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
    `nvr_log_event`: STRUCT<
      `event_time_ms`: BIGINT,
      `meta_id`: STRING,
      `is_exception`: BOOLEAN,
      `event_type`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
