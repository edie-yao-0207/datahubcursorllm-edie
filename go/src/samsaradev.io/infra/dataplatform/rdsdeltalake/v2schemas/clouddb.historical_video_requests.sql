`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`id` BIGINT,
`group_id` BIGINT,
`device_id` BIGINT,
`requested_by_user_id` BIGINT,
`start_ms` BIGINT,
`end_ms` BIGINT,
`state` BIGINT,
`asset_ms` BIGINT,
`created_at_ms` BIGINT,
`asset_ready_at_ms` BIGINT,
`completed_at_ms` BIGINT,
`get_every_nth_keyframe` BIGINT,
`slowdown_factor` BIGINT,
`is_multicam` BYTE,
`camera_info_proto` STRUCT<
  `requests`: ARRAY<
    STRUCT<
      `camera_id`: BIGINT,
      `track_id`: BIGINT
    >
  >,
  `completed_requests`: ARRAY<
    STRUCT<
      `camera_id`: BIGINT,
      `completed_at_ms`: BIGINT
    >
  >
>,
`_raw_camera_info_proto` STRING,
`wifi_retrieval` BYTE,
`is_hidden` BYTE,
`is_starred` BYTE,
`is_incognito` BYTE,
`stream_resolution_type` BIGINT,
`dashcam_report_proto` STRUCT<
  `event_id`: BIGINT,
  `unix_trigger_time_ms`: BIGINT,
  `trigger_reason`: INT,
  `report_type`: INT,
  `start_time`: BIGINT,
  `end_time`: BIGINT,
  `start_offset`: BIGINT,
  `end_offset`: BIGINT,
  `camera_type`: INT,
  `gateway_id`: BIGINT,
  `multicam_config`: STRUCT<
    `request_cameras`: ARRAY<
      STRUCT<
        `camera_device_id`: BIGINT,
        `stream_id`: BIGINT
      >
    >
  >,
  `streams`: ARRAY<INT>,
  `wifi_required`: BOOLEAN,
  `camera_still_info`: STRUCT<
    `jpeg_encode`: INT,
    `camera_still_settings`: ARRAY<
      STRUCT<
        `stream`: INT,
        `jpeg_settings`: STRUCT<
          `quality`: INT,
          `height_pixels`: INT
        >,
        `media_stream_id`: STRUCT<
          `input`: INT,
          `stream`: INT
        >
      >
    >
  >,
  `media_stream_ids`: ARRAY<
    STRUCT<
      `input`: INT,
      `stream`: INT
    >
  >,
  `data_stream_ids`: ARRAY<INT>,
  `location_decorations`: ARRAY<
    STRUCT<
      `latitude_microdegrees`: BIGINT,
      `longitude_microdegrees`: BIGINT,
      `horizontal_uncertainty_valid`: BOOLEAN,
      `horizontal_uncertainty_millimeters`: BIGINT,
      `altitude_valid`: BOOLEAN,
      `mean_sea_level_altitude_millimeters`: BIGINT,
      `altitude_uncertainty_valid`: BOOLEAN,
      `altitude_uncertainty_millimeters`: BIGINT,
      `gnss_speed_valid`: BOOLEAN,
      `gnss_speed_millimeters_per_second`: BIGINT,
      `gnss_speed_uncertainty_valid`: BOOLEAN,
      `gnss_speed_uncertainty_millimeters_per_second`: BIGINT,
      `heading_valid`: BOOLEAN,
      `heading_millidegrees`: BIGINT,
      `heading_uncertainty_valid`: BOOLEAN,
      `heading_uncertainty_millidegrees`: BIGINT,
      `time_offset_from_decorated_stat_ms`: BIGINT
    >
  >
>,
`_raw_dashcam_report_proto` STRING,
`trigger_reason` SHORT,
`date` STRING
