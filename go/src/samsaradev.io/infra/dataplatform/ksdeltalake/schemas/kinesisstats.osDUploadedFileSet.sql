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
    `uploaded_file_set`: STRUCT<
      `event_id`: BIGINT,
      `s3urls`: ARRAY<STRING>,
      `requested_file_state`: INT,
      `file_timestamp`: BIGINT,
      `asset_type`: INT,
      `gateway_id`: BIGINT,
      `camera_type`: INT,
      `report_type`: INT,
      `dashcam_file_infos`: ARRAY<
        STRUCT<
          `source`: INT,
          `filename`: STRING,
          `start_time_mono_ms`: BIGINT,
          `duration_ms`: BIGINT,
          `video`: STRUCT<
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
          `audio`: STRUCT<
            `samplerate`: BIGINT,
            `audio_codec`: INT
          >,
          `start_ago_ms`: BIGINT,
          `retrieval_result`: INT,
          `camera_device_id`: BIGINT,
          `camera_stream_id`: BIGINT,
          `total_padding_duration_ms`: BIGINT,
          `multicam_file_info`: STRUCT<
            `camera_device_id`: BIGINT,
            `camera_stream_id`: BIGINT,
            `camera_rotation`: BIGINT,
            `flip_horizontal`: BOOLEAN,
            `flip_vertical`: BOOLEAN
          >,
          `upload_duration_ms`: DECIMAL(20, 0),
          `upload_attempts`: DECIMAL(20, 0),
          `total_upload_duration_ms`: DECIMAL(20, 0),
          `file_size`: DECIMAL(20, 0),
          `media_stream_id`: STRUCT<
            `input`: INT,
            `stream`: INT
          >,
          `data_stream_id`: INT
        >
      >,
      `dashcam_metadata_stream`: BINARY,
      `needs_transcoding`: BOOLEAN,
      `error_message`: STRING,
      `triggering_report`: STRUCT<
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
      `is_final`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
