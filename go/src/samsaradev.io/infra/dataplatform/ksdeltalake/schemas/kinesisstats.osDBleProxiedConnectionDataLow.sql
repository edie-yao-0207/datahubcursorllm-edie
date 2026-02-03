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
    `ble_proxied_connection_data`: STRUCT<
      `mac_address`: DECIMAL(20, 0),
      `peripheral_ts_ms`: DECIMAL(20, 0),
      `peripheral_data`: ARRAY<BINARY>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
