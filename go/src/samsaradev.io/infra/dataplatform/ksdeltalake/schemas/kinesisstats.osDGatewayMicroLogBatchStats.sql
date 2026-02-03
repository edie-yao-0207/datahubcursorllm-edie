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
    `gateway_micro_log_batch_stats`: STRUCT<
      `logs_count`: BIGINT,
      `oldest_log_ms`: BIGINT,
      `newest_log_ms`: BIGINT,
      `logged_at_ms`: ARRAY<BIGINT>,
      `sequence_numbers`: ARRAY<BIGINT>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
