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
    `connected_device_upgrade_info`: STRUCT<
      `product_id`: BIGINT,
      `mac`: DECIMAL(20, 0),
      `upgrade_type`: INT,
      `rssi`: INT,
      `new_version`: BIGINT,
      `old_version`: BIGINT,
      `duration_ms`: BIGINT,
      `percent_complete`: BIGINT,
      `error`: BIGINT,
      `info`: STRING,
      `error_code`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
