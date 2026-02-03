`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`user_id` BIGINT,
`version` BIGINT,
`completed` BYTE,
`progress` STRUCT<
  `completed_modules`: ARRAY<INT>,
  `completed_at_ms`: BIGINT,
  `completed_modules_completed_at_ms_map`: MAP<STRING, BIGINT>
>,
`_raw_progress` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`partition` STRING
