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
    `dashcam_stream_status`: STRUCT<
      `stream_status`: ARRAY<
        STRUCT<
          `stream_source_enum`: INT,
          `app_dropped_frames`: BIGINT,
          `kernel_dropped_frames`: BIGINT,
          `recorded_frames`: BIGINT,
          `frame_latency_ms`: BIGINT,
          `recorded_key_frames`: BIGINT,
          `stream_video_info`: STRUCT<
            `bitrate`: BIGINT,
            `framerate`: BIGINT,
            `codec`: INT,
            `resolution`: INT,
            `bitrate_control`: INT,
            `idr_interval_seconds`: INT,
            `video_transform`: STRUCT<
              `flip_vertical`: BOOLEAN,
              `flip_horizontal`: BOOLEAN,
              `rotation`: INT
            >,
            `bframes_m_value`: INT
          >,
          `media_stream_id`: STRUCT<
            `input`: INT,
            `stream`: INT
          >
        >
      >,
      `cpu_utilization_millipercent`: BIGINT,
      `last_timestamp_deleted_ms`: BIGINT,
      `last_low_res_timestamp_deleted_ms`: BIGINT,
      `last_external_storage_timestamp_deleted_ms`: BIGINT,
      `last_low_res_external_storage_timestamp_deleted_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
