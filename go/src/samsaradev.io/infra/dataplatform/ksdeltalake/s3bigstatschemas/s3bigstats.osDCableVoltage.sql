`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`_s3_big_stat_file_created_at_ms` BIGINT,
`s3_proto_value` STRUCT<
  `external_voltage_samples_batch`: ARRAY<
    STRUCT<
      `offset_ms`: DECIMAL(20, 0),
      `voltage_mv`: DECIMAL(20, 0),
      `is_live`: BOOLEAN
    >
  >
>,
`_synced_at` TIMESTAMP
