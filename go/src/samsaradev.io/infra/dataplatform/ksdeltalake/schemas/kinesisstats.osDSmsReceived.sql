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
    `sms_received`: STRUCT<
      `sent_utc_ms`: DECIMAL(20, 0),
      `payload`: STRING,
      `originating_address`: STRING,
      `mcc`: BIGINT,
      `mnc`: BIGINT,
      `cell_id`: BIGINT,
      `tac`: BIGINT,
      `correlation_id`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
