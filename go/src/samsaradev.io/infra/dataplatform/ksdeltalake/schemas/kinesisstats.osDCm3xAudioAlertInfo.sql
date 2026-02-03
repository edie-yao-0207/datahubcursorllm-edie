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
    `cm3x_audio_alert_info`: STRUCT<
      `trigger_time_ms`: BIGINT,
      `playback_start_ms`: BIGINT,
      `playback_end_ms`: BIGINT,
      `audio_duration_ms`: BIGINT,
      `interrupted`: BOOLEAN,
      `event_type`: INT,
      `frame_ms`: BIGINT,
      `frame_processing_start_ms`: BIGINT,
      `generic_event_audio_filename`: STRING,
      `severity`: INT,
      `config`: STRUCT<
        `event_type`: INT,
        `severity`: INT,
        `enabled`: BOOLEAN,
        `audio_file_path`: STRING,
        `volume`: BIGINT,
        `priority`: BIGINT,
        `repeat_config`: STRUCT<
          `enabled`: BOOLEAN,
          `count`: BIGINT,
          `interval_ms`: BIGINT
        >,
        `is_voiceless`: BOOLEAN
      >,
      `last_gps`: STRUCT<
        `latitude_nanodegrees`: BIGINT,
        `longitude_nanodegrees`: BIGINT,
        `speed_milliknots`: BIGINT,
        `heading_millidegrees`: BIGINT,
        `complete_fix`: BOOLEAN,
        `altitude_millimeters`: INT,
        `hdop_thousandths`: BIGINT,
        `vdop_thousandths`: BIGINT,
        `utc_ms`: DECIMAL(20, 0),
        `accuracy_millimeters`: BIGINT
      >,
      `device_write_start_ms`: BIGINT,
      `shadow_mode`: BOOLEAN,
      `queued`: BOOLEAN,
      `cloud_trigger_reason`: INT,
      `task_processing_started_at_ms`: BIGINT,
      `request_received_at_ms`: BIGINT,
      `trigger_gateway_id`: BIGINT,
      `had_to_interrupt`: BOOLEAN,
      `first_play`: BOOLEAN,
      `detection_id`: DECIMAL(20, 0),
      `cloud_trigger_alert_uuid`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
