`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`planner_uuid` STRING,
`created_at` TIMESTAMP,
`created_by` BIGINT,
`updated_by` BIGINT,
`updated_at` TIMESTAMP,
`server_deleted_at` TIMESTAMP,
`server_deleted_by` BIGINT,
`name` STRING,
`address` STRING,
`planner_notes` STRING,
`standard_instructions` STRING,
`lat` DOUBLE,
`lng` DOUBLE,
`is_depot` BYTE,
`additional_service_time_enabled` BYTE,
`additional_service_time_seconds` BIGINT,
`depot_shift_start_time_seconds` BIGINT,
`depot_shift_end_time_seconds` BIGINT,
`protos` STRUCT<
  `markers`: ARRAY<
    STRUCT<
      `lat`: DOUBLE,
      `lng`: DOUBLE,
      `label`: STRING,
      `notes`: STRING
    >
  >,
  `street_view`: STRUCT<
    `enabled`: BOOLEAN,
    `lat`: DOUBLE,
    `lng`: DOUBLE,
    `heading`: DOUBLE,
    `pitch`: DOUBLE,
    `zoom`: DOUBLE
  >,
  `geofence`: STRUCT<
    `polygon`: ARRAY<
      STRUCT<
        `lat`: DOUBLE,
        `lng`: DOUBLE
      >
    >,
    `radius_meters`: DOUBLE,
    `center_lat`: DOUBLE,
    `center_lng`: DOUBLE,
    `label`: STRING,
    `notes`: STRING
  >,
  `service_windows`: ARRAY<
    STRUCT<
      `mon`: BOOLEAN,
      `tue`: BOOLEAN,
      `wed`: BOOLEAN,
      `thu`: BOOLEAN,
      `fri`: BOOLEAN,
      `sat`: BOOLEAN,
      `sun`: BOOLEAN,
      `start_time_seconds`: BIGINT,
      `end_time_seconds`: BIGINT
    >
  >,
  `required_skill_uuids`: ARRAY<BINARY>,
  `location_position`: INT,
  `location_priority`: BIGINT
>,
`_raw_protos` STRING,
`external_id` STRING,
`ignore_order_service_time_enabled` BYTE,
`order_service_time_mode` BIGINT,
`order_service_time_fixed_seconds` BIGINT,
`partition` STRING
