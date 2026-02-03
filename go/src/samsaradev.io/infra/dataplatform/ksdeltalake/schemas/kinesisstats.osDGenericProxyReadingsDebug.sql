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
    `generic_proxy_readings_debug`: STRUCT<
      `samsara_peripheral_type_id`: BIGINT,
      `last_seen_data`: BINARY,
      `timestamp_last_seen_data_ms`: DECIMAL(20, 0),
      `watch_window_duration_ms`: BIGINT,
      `watch_window_count`: BIGINT,
      `gateway_location`: STRUCT<
        `latitude_nanodegrees`: BIGINT,
        `longitude_nanodegrees`: BIGINT,
        `altitude_millimeters`: INT,
        `accuracy_millimeters`: BIGINT,
        `gps_speed_millimeters_per_second`: BIGINT,
        `in_motion`: BOOLEAN
      >,
      `ble_metadata`: STRUCT<
        `ble_address`: DECIMAL(20, 0),
        `ble_rssi_dbm`: INT
      >,
      `j1939_metadata`: STRUCT<
        `pgn`: BIGINT,
        `source_address`: BIGINT,
        `source_name`: DECIMAL(20, 0),
        `bus_id`: BIGINT
      >,
      `modbus_metadata`: STRUCT<
        `server_address`: BIGINT,
        `function_code`: BIGINT,
        `register_address`: BIGINT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
