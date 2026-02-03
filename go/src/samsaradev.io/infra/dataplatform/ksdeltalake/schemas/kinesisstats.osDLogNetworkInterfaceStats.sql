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
    `network_interface_stats`: STRUCT<
      `interface`: INT,
      `rx_bytes_per_sec`: DECIMAL(20, 0),
      `tx_bytes_per_sec`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
