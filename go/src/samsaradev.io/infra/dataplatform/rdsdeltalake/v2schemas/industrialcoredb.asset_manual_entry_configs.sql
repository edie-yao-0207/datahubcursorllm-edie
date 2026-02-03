`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`asset_uuid` STRING,
`machine_input_id` BIGINT,
`created_at` TIMESTAMP,
`created_by` BIGINT,
`config_proto` STRUCT<
  `section_id`: BIGINT,
  `section_name`: STRING,
  `section_index`: BIGINT
>,
`_raw_config_proto` STRING,
`partition` STRING
