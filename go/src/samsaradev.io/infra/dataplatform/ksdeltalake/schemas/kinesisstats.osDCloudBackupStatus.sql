`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`value` STRUCT<
  `date`: STRING,
  `time`: BIGINT,
  `received_delta_seconds`: BIGINT,
  `is_start`: BOOLEAN,
  `is_end`: BOOLEAN,
  `is_databreak`: BOOLEAN,
  `int_value`: BIGINT,
  `double_value`: DOUBLE,
  `proto_value`: STRUCT<
    `cloud_backup_status`: STRUCT<
      `enabled`: BOOLEAN,
      `failed_attempts`: BIGINT,
      `failed_uploads`: BIGINT,
      `failed_upload_bytes`: BIGINT,
      `concurrent_uploads`: BIGINT,
      `max_concurrent_uploads`: BIGINT,
      `retry_limit`: BIGINT,
      `upload_asset_size_bytes`: DECIMAL(20, 0),
      `duration_ms`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
