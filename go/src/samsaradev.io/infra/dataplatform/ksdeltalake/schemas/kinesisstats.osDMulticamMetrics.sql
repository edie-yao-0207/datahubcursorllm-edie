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
    `multicam_metrics`: STRUCT<
      `pipeline_command_exit`: STRUCT<
        `command`: INT,
        `command_args`: STRING,
        `exit_reason`: INT
      >,
      `deserializer_reset`: STRUCT<`reset_message`: STRING>,
      `still_capture_failure`: STRUCT<
        `camera_device_id`: BIGINT,
        `stream_id`: BIGINT,
        `still_type`: INT,
        `error_message`: STRING
      >,
      `storage_cleanup`: STRUCT<`cleanup_duration_ms`: DECIMAL(20, 0)>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
