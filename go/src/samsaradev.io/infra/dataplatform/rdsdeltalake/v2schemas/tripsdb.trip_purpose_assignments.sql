`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`group_id` BIGINT,
`device_id` BIGINT,
`assignment_start_ms` BIGINT,
`version` INT,
`time_ms` BIGINT,
`driver_id` BIGINT,
`start_odometer_meters` BIGINT,
`end_odometer_meters` BIGINT,
`start_gps_distance_meters` BIGINT,
`end_gps_distance_meters` BIGINT,
`trip_purpose` INT,
`metadata` STRING,
`comment` STRING,
`reason` STRING,
`business_partner` STRING,
`company` STRING,
`reviewed_at` TIMESTAMP,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`edit_history` STRUCT<
  `entries`: ARRAY<
    STRUCT<
      `updated_at`: TIMESTAMP,
      `trip_purpose`: STRUCT<`value`: INT>,
      `reason`: STRING,
      `business_partner`: STRING,
      `company`: STRING,
      `comment`: STRING
    >
  >
>,
`_raw_edit_history` STRING,
`date` STRING
