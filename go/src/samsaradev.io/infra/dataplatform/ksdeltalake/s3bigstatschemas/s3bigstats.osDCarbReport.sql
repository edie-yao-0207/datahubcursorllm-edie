`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`_s3_big_stat_file_created_at_ms` BIGINT,
`s3_proto_value` STRUCT<
  `carb_report`: STRUCT<
    `report_version_number`: DECIMAL(20, 0),
    `session_message_id`: DECIMAL(20, 0),
    `header`: STRUCT<
      `carb_formatted_vin`: STRING,
      `sae_protocol`: INT,
      `odometer_meters`: DECIMAL(20, 0),
      `total_engine_runtime_seconds`: DECIMAL(20, 0),
      `device_firmware_version`: STRING,
      `firmware_verification_number`: STRING,
      `record_id`: DECIMAL(20, 0),
      `start_time_offset_ms`: BIGINT,
      `bitrate`: BIGINT,
      `interface`: INT
    >,
    `data_frames`: ARRAY<
      STRUCT<
        `time_offset_ms`: BIGINT,
        `message_type`: INT,
        `can_id`: BIGINT,
        `data`: BINARY
      >
    >
  >
>,
`_synced_at` TIMESTAMP
