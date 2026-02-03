`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`wake_on_motion_enabled` BYTE,
`wake_on_motion_interval_sec` BIGINT,
`ping_interval_sec` BIGINT,
`customizable_ping_schedule_enabled` BYTE,
`customizable_ping_schedule_proto` STRUCT<
  `scheduled_wake_events`: ARRAY<
    STRUCT<
      `hour_of_day`: BIGINT,
      `minute_of_hour`: BIGINT,
      `days_of_week`: ARRAY<INT>
    >
  >
>,
`_raw_customizable_ping_schedule_proto` STRING,
`wake_on_motion_override_proto` STRUCT<
  `configs`: ARRAY<
    STRUCT<
      `wake_on_motion_enabled`: BOOLEAN,
      `tag_ids`: ARRAY<BIGINT>,
      `device_ids`: ARRAY<BIGINT>,
      `check_in_rate`: BIGINT
    >
  >
>,
`_raw_wake_on_motion_override_proto` STRING,
`partition` STRING
