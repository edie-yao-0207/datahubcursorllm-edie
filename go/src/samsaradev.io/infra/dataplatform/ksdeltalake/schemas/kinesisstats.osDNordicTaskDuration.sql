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
    `nordic_task_duration`: STRUCT<
      `task_type`: INT,
      `started_time_ms`: DECIMAL(20, 0),
      `duration_ms`: BIGINT,
      `status`: BIGINT,
      `extra_metric_type`: INT,
      `extra_metric_u32`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
