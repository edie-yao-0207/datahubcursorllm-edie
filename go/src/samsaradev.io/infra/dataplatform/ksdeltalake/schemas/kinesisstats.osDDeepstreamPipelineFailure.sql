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
    `deepstream_pipeline_failure`: STRUCT<
      `failed_pipeline`: INT,
      `camera_device_id`: BIGINT,
      `stream_id`: BIGINT,
      `channel_id`: INT,
      `failure_description`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
