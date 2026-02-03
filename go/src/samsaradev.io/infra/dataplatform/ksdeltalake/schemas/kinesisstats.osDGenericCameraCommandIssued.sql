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
    `generic_camera_command_issued`: STRUCT<
      `org_id`: BIGINT,
      `hostname`: STRING,
      `endpoint`: STRING,
      `camera`: STRUCT<`camera_device_id`: BIGINT>,
      `command_query_params`: ARRAY<
        STRUCT<
          `name`: STRING,
          `value`: STRING
        >
      >,
      `full_url_issued`: STRING,
      `response`: STRING,
      `error`: STRING,
      `issued_at_ms`: BIGINT,
      `duration_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
