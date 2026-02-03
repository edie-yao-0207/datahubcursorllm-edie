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
    `diagnostic_lock_set_info`: STRUCT<
      `obd_value`: DECIMAL(20, 0),
      `priority`: DECIMAL(20, 0),
      `chosen_tx_id`: DECIMAL(20, 0),
      `seen_tx_ids`: ARRAY<DECIMAL(20, 0)>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
