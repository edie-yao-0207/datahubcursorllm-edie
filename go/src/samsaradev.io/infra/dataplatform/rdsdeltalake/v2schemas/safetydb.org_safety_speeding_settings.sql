`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`uuid` STRING,
`org_id` BIGINT,
`created_at_ms` BIGINT,
`created_by` BIGINT,
`speeding_settings_proto` STRUCT<
  `max_speed_setting`: STRUCT<
    `enabled`: BOOLEAN,
    `time_before_alert_ms`: BIGINT,
    `kmph_threshold`: FLOAT,
    `in_cab_audio_alerts_enabled`: BOOLEAN,
    `send_to_safety_inbox`: BOOLEAN,
    `auto_add_to_coaching`: BOOLEAN
  >,
  `in_cab_severity_alert_setting`: STRUCT<
    `enabled`: BOOLEAN,
    `alert_at_severity_level`: INT,
    `time_before_alert_ms`: BIGINT
  >,
  `severity_settings_speed_over_limit_unit`: INT,
  `severity_settings`: ARRAY<
    STRUCT<
      `severity_level`: INT,
      `speed_over_limit_threshold`: FLOAT,
      `time_before_alert_ms`: BIGINT,
      `enabled`: BOOLEAN,
      `send_to_safety_inbox`: BOOLEAN,
      `auto_add_to_coaching`: BOOLEAN,
      `evidence_based_speeding_enabled`: BOOLEAN
    >
  >,
  `csl_enabled`: BOOLEAN
>,
`_raw_speeding_settings_proto` STRING,
`date` STRING
