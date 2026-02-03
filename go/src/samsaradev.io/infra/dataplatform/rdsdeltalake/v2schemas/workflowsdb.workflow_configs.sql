`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`uuid` STRING,
`org_id` BIGINT,
`name` STRING,
`description` STRING,
`disabled` BYTE,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`deleted_at` TIMESTAMP,
`deleted_by_user_id` BIGINT,
`is_admin` BYTE,
`severity` BYTE,
`source_alert_id` BIGINT,
`proto_data` STRUCT<
  `time_range_settings`: STRUCT<
    `applies_in_range`: BOOLEAN,
    `time_ranges`: ARRAY<
      STRUCT<
        `days_of_week`: ARRAY<INT>,
        `start_time_of_day_ms`: DECIMAL(20, 0),
        `end_time_of_day_ms`: DECIMAL(20, 0)
      >
    >
  >,
  `max_data_age_ms`: BIGINT
>,
`_raw_proto_data` STRING,
`source_workflow_uuid` STRING,
`partition` STRING
