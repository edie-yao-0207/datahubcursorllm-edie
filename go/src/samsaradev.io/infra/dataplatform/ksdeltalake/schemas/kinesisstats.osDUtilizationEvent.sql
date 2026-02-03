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
    `utilization_event`: STRUCT<
      `utilization_state`: INT,
      `uuid`: STRING,
      `start_ms`: DECIMAL(20, 0),
      `duration_ms`: DECIMAL(20, 0),
      `use_case_uuid`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
