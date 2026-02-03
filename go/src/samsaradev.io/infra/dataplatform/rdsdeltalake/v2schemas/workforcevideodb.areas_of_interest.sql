`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`uuid` STRING,
`name` STRING,
`stream_id` BIGINT,
`org_id` BIGINT,
`polygon` STRUCT<
  `vertices`: ARRAY<
    STRUCT<
      `x`: DOUBLE,
      `y`: DOUBLE
    >
  >
>,
`_raw_polygon` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`deleted_at` TIMESTAMP,
`polygon_type` BYTE,
`original_polygon_type` BYTE,
`partition` STRING
