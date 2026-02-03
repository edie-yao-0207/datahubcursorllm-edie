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
    `deepstream_pipeline_stats`: STRUCT<
      `aggregates`: ARRAY<
        STRUCT<
          `camera_device_id`: BIGINT,
          `stream_id`: BIGINT,
          `channel_id`: INT,
          `start_time_ms`: BIGINT,
          `duration_ms`: BIGINT,
          `monitors`: ARRAY<
            STRUCT<
              `stage`: INT,
              `interval_buffer_count`: BIGINT
            >
          >
        >
      >,
      `streams`: ARRAY<
        STRUCT<
          `camera_device_id`: BIGINT,
          `stream_id`: BIGINT,
          `channel_id`: INT,
          `start_time_ms`: BIGINT,
          `duration_ms`: BIGINT,
          `monitors`: ARRAY<
            STRUCT<
              `stage`: INT,
              `interval_buffer_count`: BIGINT
            >
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
