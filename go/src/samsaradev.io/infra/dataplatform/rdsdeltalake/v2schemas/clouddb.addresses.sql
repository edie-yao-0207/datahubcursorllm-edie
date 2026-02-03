`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`id` BIGINT,
`group_id` BIGINT,
`created_at` TIMESTAMP,
`created_by` BIGINT,
`name` STRING,
`address` STRING,
`geocoded_at` TIMESTAMP,
`latitude` DOUBLE,
`longitude` DOUBLE,
`error` STRING,
`radius` BIGINT,
`geofence_proto` STRUCT<
  `geofence_polygon`: STRUCT<
    `name`: STRING,
    `vertices`: ARRAY<
      STRUCT<
        `lat`: DOUBLE,
        `lng`: DOUBLE
      >
    >,
    `bounding_box`: STRUCT<
      `lat_max`: DOUBLE,
      `lng_max`: DOUBLE,
      `lat_min`: DOUBLE,
      `lng_min`: DOUBLE
    >
  >,
  `geofence_settings`: STRUCT<`show_addresses`: BOOLEAN>
>,
`_raw_geofence_proto` STRING,
`notes` STRING,
`type` BIGINT,
`auto_dismiss_rolled_stops` BYTE,
`is_duplicate_address` BYTE,
`partition` STRING
