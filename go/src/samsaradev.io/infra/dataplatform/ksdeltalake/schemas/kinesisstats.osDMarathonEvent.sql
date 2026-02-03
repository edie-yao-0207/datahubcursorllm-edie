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
    `marathon_event`: STRUCT<
      `event_type`: INT,
      `event_verbosity`: INT,
      `event_id`: BIGINT,
      `code_version`: INT,
      `function_name`: STRING,
      `file_line_number`: BIGINT,
      `generic_int64`: ARRAY<BIGINT>,
      `generic_string`: STRING,
      `generic_bytes`: BINARY,
      `soc`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
