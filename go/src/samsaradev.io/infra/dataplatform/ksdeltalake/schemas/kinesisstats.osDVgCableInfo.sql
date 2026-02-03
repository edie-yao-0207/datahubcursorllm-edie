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
    `vg_cable_info`: STRUCT<
      `vendor_id`: STRING,
      `mfg_date_ms`: BIGINT,
      `obd_cable_id`: BIGINT,
      `revision_number`: BIGINT,
      `valid`: INT,
      `invalid_raw`: BINARY
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
