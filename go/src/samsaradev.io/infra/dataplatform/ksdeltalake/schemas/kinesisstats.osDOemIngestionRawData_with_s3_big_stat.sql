`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`_s3_big_stat_file_created_at_ms` BIGINT,
`s3_proto_value` STRUCT<
  `oem_ingestion_raw_data_human_readable`: STRUCT<`payload_text`: STRING>
>,
`_synced_at` TIMESTAMP,
`value` STRUCT<
  `date`: STRING,
  `time`: BIGINT,
  `is_start`: BOOLEAN,
  `is_end`: BOOLEAN,
  `is_databreak`: BOOLEAN,
  `int_value`: BIGINT,
  `double_value`: DOUBLE,
  `proto_value`: STRUCT<
    `oem_ingestion_raw_data`: STRUCT<
      `oem_type`: BIGINT,
      `schema_version`: BIGINT,
      `source_id`: STRING,
      `payload_raw`: BINARY,
      `trace_id`: STRING
    >
  >,
  `received_delta_seconds`: BIGINT
>
