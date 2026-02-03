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
    `can_bitrate_detection_v2_results`: STRUCT<
      `bus_id`: INT,
      `detected_bitrate`: BIGINT,
      `listen_only_detection_error`: STRING,
      `one_shot_detection_error`: STRING,
      `listen_only_detection_metadata`: ARRAY<
        STRUCT<
          `data_frame_count`: BIGINT,
          `error_frame_count`: BIGINT,
          `data_frame_bit_count`: BIGINT,
          `runtime_ms`: DECIMAL(20, 0),
          `bitrate`: BIGINT,
          `protocol_bit0_frame_count`: BIGINT,
          `bit_stuffing_error_frame_count`: BIGINT
        >
      >,
      `one_shot_detection_metadata`: ARRAY<
        STRUCT<
          `bitrate`: BIGINT,
          `runtime_ms`: BIGINT,
          `error_frame_count`: BIGINT,
          `data_frame_count`: BIGINT,
          `sent_one_shot_frame_count`: BIGINT
        >
      >,
      `detection_params`: STRUCT<
        `possible_bitrates`: ARRAY<BIGINT>,
        `max_listen_ms`: DECIMAL(20, 0),
        `min_data_frames`: BIGINT,
        `min_data_frames_per_error_frame_ratio`: BIGINT,
        `active_mode_min_bitrate_percent`: BIGINT,
        `one_shot_send_frame_interval_ms`: BIGINT,
        `one_shot_max_send_frame_count`: BIGINT
      >,
      `primary_bus_bitrate_detection_behavior`: INT,
      `bitrate_detect_method_that_passed`: INT,
      `detected_polarity`: INT,
      `can_interface`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
