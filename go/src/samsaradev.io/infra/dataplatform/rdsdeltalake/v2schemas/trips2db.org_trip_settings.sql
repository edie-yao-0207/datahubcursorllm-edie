`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`version` INT,
`updated_at_ms` BIGINT,
`proto` STRUCT<
  `speed_threshold`: DOUBLE,
  `min_datapoint_count`: BIGINT,
  `start_speed_threshold`: DOUBLE,
  `end_speed_threshold`: DOUBLE,
  `ignore_administrative_boundaries`: BOOLEAN
>,
`_raw_proto` STRING,
`partition` STRING
