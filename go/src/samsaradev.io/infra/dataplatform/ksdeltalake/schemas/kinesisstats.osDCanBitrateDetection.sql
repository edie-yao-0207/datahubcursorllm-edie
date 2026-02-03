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
    `can_bitrate_detection_results`: STRUCT<
      `vehicle_diagnostic_bus`: INT,
      `detected_bitrate`: BIGINT,
      `detected_active_required`: BOOLEAN,
      `detection_error`: STRING,
      `detection_params`: STRUCT<
        `possible_bitrates`: ARRAY<BIGINT>,
        `max_listen_ms`: BIGINT,
        `min_data_frames`: BIGINT,
        `min_data_frames_per_error_frame_ratio`: BIGINT,
        `active_mode_min_bitrate_percent`: BIGINT
      >,
      `detection_metadata`: STRUCT<
        `data_frame_count`: BIGINT,
        `error_frame_count`: BIGINT,
        `data_frame_bit_count`: BIGINT,
        `runtime_ms`: BIGINT,
        `bitrate`: BIGINT,
        `protocol_bit0_frame_count`: BIGINT,
        `bit_stuffing_error_frame_count`: BIGINT
      >,
      `detection_metadata_v2`: ARRAY<
        STRUCT<
          `data_frame_count`: BIGINT,
          `error_frame_count`: BIGINT,
          `data_frame_bit_count`: BIGINT,
          `runtime_ms`: BIGINT,
          `bitrate`: BIGINT,
          `protocol_bit0_frame_count`: BIGINT,
          `bit_stuffing_error_frame_count`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
