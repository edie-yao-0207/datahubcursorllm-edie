`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`id` BIGINT,
`dvir_id` BIGINT,
`compartment_type` BYTE,
`defect_type` INT,
`comment` STRING,
`proto` STRUCT<
  `defect_photos`: ARRAY<
    STRUCT<
      `uuid`: STRING,
      `created_at_ms`: BIGINT,
      `org_id`: BIGINT
    >
  >
>,
`_raw_proto` STRING,
`dvir_defect_type_uuid` STRING,
`org_id` BIGINT,
`vehicle_id` BIGINT,
`deprecated_trailer_id` BIGINT,
`trailer_name` STRING,
`resolved` BYTE,
`resolved_at` TIMESTAMP,
`resolved_by_driver_id` BIGINT,
`resolved_by_user_id` BIGINT,
`created_at` TIMESTAMP,
`trailer_device_id` BIGINT,
`mechanic_notes` STRING,
`mechanic_notes_ms` TIMESTAMP,
`updated_at` TIMESTAMP,
`date` STRING
