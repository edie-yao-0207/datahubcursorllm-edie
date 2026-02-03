`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`name` STRING,
`data` STRUCT<
  `machine_input_ids`: ARRAY<BIGINT>,
  `columns`: ARRAY<
    STRUCT<
      `data_input_id`: BIGINT,
      `summary_functions`: ARRAY<INT>
    >
  >,
  `data_interval_ms`: BIGINT
>,
`_raw_data` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`deleted` BYTE,
`partition` STRING
