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
    `audio_alert_record`: STRUCT<
      `event_type`: INT,
      `cloud_trigger_reason`: INT,
      `audio_alert_config`: STRUCT<
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
      `trigger_gateway_id`: BIGINT,
      `result`: INT,
      `queued`: BOOLEAN,
      `sliding_window_rate_limit_rule`: STRUCT<
        `rule_enabled`: INT,
        `window_ms`: BIGINT,
        `max_alert_count`: BIGINT,
        `event_type`: INT,
        `cloud_trigger_reason`: INT
      >,
      `detection_id`: DECIMAL(20, 0),
      `audio_file_info`: STRUCT<
        `filename`: STRING,
        `file_sha256`: BINARY,
        `version`: BIGINT
      >,
      `cloud_trigger_alert_uuid`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
