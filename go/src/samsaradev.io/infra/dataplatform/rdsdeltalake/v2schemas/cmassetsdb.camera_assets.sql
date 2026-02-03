`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`device_id` BIGINT,
`asset_ms` BIGINT,
`asset_type` SHORT,
`recall_id` BIGINT,
`trigger_reason` SHORT,
`camera_event_proto` STRUCT<
  `camera_event`: STRUCT<
    `recall_id`: STRING,
    `unix_event_time_ms`: STRING,
    `asset_type`: INT,
    `trigger_reason`: INT
  >
>,
`_raw_camera_event_proto` STRING,
`uploaded_file_set_proto` STRUCT<
  `uploaded_file_set`: STRUCT<
    `event_id`: STRING,
    `s3urls`: ARRAY<STRING>,
    `requested_file_state`: INT,
    `file_timestamp`: STRING,
    `asset_type`: INT,
    `gateway_id`: STRING,
    `camera_type`: INT,
    `report_type`: INT,
    `dashcam_file_infos`: ARRAY<
      STRUCT<
        `source`: INT,
        `filename`: STRING,
        `start_time_mono_ms`: STRING,
        `duration_ms`: STRING,
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
        `start_ago_ms`: STRING,
        `retrieval_result`: INT,
        `camera_device_id`: STRING,
        `camera_stream_id`: STRING,
        `total_padding_duration_ms`: STRING,
        `multicam_file_info`: STRUCT<
          `camera_device_id`: STRING,
          `camera_stream_id`: STRING,
          `camera_rotation`: BIGINT,
          `flip_horizontal`: BOOLEAN,
          `flip_vertical`: BOOLEAN
        >,
        `upload_duration_ms`: STRING,
        `upload_attempts`: STRING,
        `total_upload_duration_ms`: STRING,
        `file_size`: STRING,
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
    >,
    `is_final`: BOOLEAN
  >
>,
`_raw_uploaded_file_set_proto` STRING,
`is_deleted` BYTE,
`dashcam_report_proto` STRUCT<
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
`_raw_dashcam_report_proto` STRING,
`date` STRING
