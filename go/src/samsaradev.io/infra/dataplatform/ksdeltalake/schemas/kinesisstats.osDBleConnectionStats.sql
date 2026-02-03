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
    `ble_connection_stats`: STRUCT<
      `slot_event`: STRUCT<
        `mac`: DECIMAL(20, 0),
        `product_id`: BIGINT,
        `state`: INT,
        `reason`: INT,
        `rssi_dbm`: INT
      >,
      `used_slots`: ARRAY<
        STRUCT<`product_id`: BIGINT>
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
