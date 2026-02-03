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
    `ecu_fuel_level_stats`: STRUCT<
      `ecu_fuel_levels`: ARRAY<
        STRUCT<
          `latest_fuel_level_millipercent`: DECIMAL(20, 0),
          `min_fuel_level_millipercent`: DECIMAL(20, 0),
          `max_fuel_level_millipercent`: DECIMAL(20, 0),
          `mean_fuel_level_millipercent`: DECIMAL(20, 0),
          `count_seen`: DECIMAL(20, 0),
          `fuel_level_method`: INT,
          `fuel_level_source`: STRUCT<
            `vehicle_diagnostic_bus`: INT,
            `ecu_id`: DECIMAL(20, 0),
            `msg_id`: DECIMAL(20, 0),
            `obd_protocol`: INT
          >
        >
      >,
      `metadata`: STRUCT<
        `configured_log_interval_ms`: BIGINT,
        `configured_fuel_tank_volume_ml`: DECIMAL(20, 0),
        `actual_log_interval_ms`: BIGINT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
