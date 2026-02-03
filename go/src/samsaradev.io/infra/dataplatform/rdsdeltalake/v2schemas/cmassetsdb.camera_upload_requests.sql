`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`device_id` BIGINT,
`asset_ms` BIGINT,
`state` BIGINT,
`state_updated_at` TIMESTAMP,
`request_sent_at` TIMESTAMP,
`request_proto` STRUCT<
  `dashcam_report`: STRUCT<
    `event_id`: STRING,
    `unix_trigger_time_ms`: STRING,
    `trigger_reason`: INT,
    `report_type`: INT,
    `start_time`: STRING,
    `end_time`: STRING,
    `start_offset`: STRING,
    `end_offset`: STRING,
    `camera_type`: INT,
    `gateway_id`: STRING,
    `multicam_config`: STRUCT<
      `request_cameras`: ARRAY<
        STRUCT<
          `camera_device_id`: STRING,
          `stream_id`: STRING
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
        `latitude_microdegrees`: STRING,
        `longitude_microdegrees`: STRING,
        `horizontal_uncertainty_valid`: BOOLEAN,
        `horizontal_uncertainty_millimeters`: BIGINT,
        `altitude_valid`: BOOLEAN,
        `mean_sea_level_altitude_millimeters`: STRING,
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
        `time_offset_from_decorated_stat_ms`: STRING
      >
    >
  >
>,
`_raw_request_proto` STRING,
`date` STRING
