`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`id` BIGINT,
`org_id` BIGINT,
`group_id` BIGINT,
`user_id` BIGINT,
`report_type` INT,
`last_sent_at_ms` BIGINT,
`marshaled_proto` STRUCT<
  `plan_id`: BIGINT,
  `group_id`: BIGINT,
  `user_id`: BIGINT,
  `report_type`: BIGINT,
  `last_sent_at_ms`: BIGINT,
  `next_send_at_ms`: BIGINT,
  `repeat_type`: BIGINT,
  `repeat`: STRUCT<
    `time_of_day_ms`: BIGINT,
    `day_of_week`: BIGINT
  >,
  `is_timezone_migrated`: BOOLEAN,
  `is_timezone_migrated_march_2017`: BOOLEAN,
  `is_timezone_migrated_negative_time_of_day_ms`: BOOLEAN,
  `send_offset_ms`: DECIMAL(20, 0),
  `report_format`: INT,
  `creator_user_id`: BIGINT,
  `owner_user_id`: BIGINT,
  `report_duration_ms`: BIGINT
>,
`_raw_marshaled_proto` STRING,
`repeat_type` BIGINT,
`next_send_at_ms` BIGINT,
`is_superuser_only` BYTE,
`extra_args` STRUCT<
  `kv_pairs`: ARRAY<
    STRUCT<
      `key`: STRING,
      `value`: STRING
    >
  >
>,
`_raw_extra_args` STRING,
`name` STRING,
`custom_report_config_id` STRING,
`creator_user_id` BIGINT,
`owner_user_id` BIGINT,
`created_at_ms` BIGINT,
`updated_at_ms` BIGINT,
`partition` STRING
