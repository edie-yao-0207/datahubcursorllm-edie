`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`address_id` BIGINT,
`safety_event_settings_proto` STRUCT<
  `enabled_accel_types`: ARRAY<INT>,
  `disable_audio_alerts`: BOOLEAN
>,
`_raw_safety_event_settings_proto` STRING,
`created_at` BIGINT,
`updated_at` BIGINT,
`partition` STRING
