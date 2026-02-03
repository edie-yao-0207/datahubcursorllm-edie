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
    `cloudbackup_upload_attempt_info`: STRUCT<
      `source`: STRUCT<
        `type`: INT,
        `nvr`: STRUCT<
          `camera_device_id`: BIGINT,
          `stream_id`: BIGINT,
          `channel`: INT
        >
      >,
      `request_id`: DECIMAL(20, 0),
      `media_type`: INT,
      `state`: INT,
      `error`: INT,
      `error_description`: STRING,
      `retry_count`: BIGINT,
      `upload_url`: STRING,
      `upload_attempt_start_ms`: DECIMAL(20, 0),
      `upload_attempt_duration_ms`: DECIMAL(20, 0),
      `upload_file_size_bytes`: DECIMAL(20, 0),
      `memory_percentage`: BIGINT,
      `video_segment_upload`: STRUCT<
        `start_ms`: DECIMAL(20, 0),
        `duration_ms`: DECIMAL(20, 0)
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
