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
    `gateway_micro_wifi_scan`: STRUCT<
      `found_additional_wifi_aps`: BOOLEAN,
      `wifis`: ARRAY<
        STRUCT<
          `bssid`: DECIMAL(20, 0),
          `rssi_dbm`: INT,
          `channel`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
