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
    `dashcam_report`: STRUCT<
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
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
