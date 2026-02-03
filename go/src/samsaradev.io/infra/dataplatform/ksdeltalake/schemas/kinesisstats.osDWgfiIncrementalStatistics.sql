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
    `wgfi_incremental_statistics`: STRUCT<
      `packets_sent_count`: BIGINT,
      `packets_received_count`: BIGINT,
      `responses_missing_count`: BIGINT,
      `bad_checksums_count`: BIGINT,
      `report_window_ms`: BIGINT,
      `errors`: ARRAY<
        STRUCT<
          `error_code`: INT,
          `count`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
