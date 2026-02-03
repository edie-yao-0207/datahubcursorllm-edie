`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`_s3_big_stat_file_created_at_ms` BIGINT,
`s3_proto_value` STRUCT<
  `reversing_event_metadata`: STRUCT<
    `ecu_speed`: STRUCT<
      `offset_from_event_ms`: BIGINT,
      `delta_from_previous_ms`: ARRAY<DECIMAL(20, 0)>,
      `speed_kmph`: ARRAY<BIGINT>
    >,
    `gear`: STRUCT<
      `offset_from_event_ms`: BIGINT,
      `delta_from_previous_ms`: ARRAY<DECIMAL(20, 0)>,
      `gear_value`: ARRAY<BIGINT>
    >,
    `location`: STRUCT<
      `start_offset_from_event_ms`: BIGINT,
      `delta_from_previous_ms`: ARRAY<DECIMAL(20, 0)>,
      `location`: ARRAY<
        STRUCT<
          `latitude_nano_deg`: BIGINT,
          `longitude_nano_deg`: BIGINT,
          `accuracy_valid`: BOOLEAN,
          `accuracy_m`: BIGINT
        >
      >
    >
  >
>,
`_synced_at` TIMESTAMP
