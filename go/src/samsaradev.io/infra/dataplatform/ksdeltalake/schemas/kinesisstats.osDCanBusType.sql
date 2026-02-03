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
    `can_bus_info`: STRUCT<
      `bitrate`: BIGINT,
      `can_bus_type_enum`: BIGINT,
      `vehicle_bus_settings`: STRUCT<
        `can_config`: STRUCT<
          `can_type`: INT,
          `high_speed_can_config`: STRUCT<`terminate`: BOOLEAN>,
          `bitrate`: BIGINT,
          `listen_only`: BOOLEAN,
          `can_id_type`: INT,
          `can_polarity`: INT
        >,
        `kline_config`: STRUCT<
          `do_not_run_init`: BOOLEAN,
          `kline_protocol`: INT,
          `intermessage_delay_ms`: DECIMAL(20, 0)
        >,
        `j1850_config`: STRUCT<
          `j1850_mode`: INT,
          `txid`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
