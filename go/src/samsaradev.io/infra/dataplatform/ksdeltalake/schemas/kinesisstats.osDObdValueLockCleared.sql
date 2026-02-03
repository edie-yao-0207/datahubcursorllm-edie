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
    `obd_value_lock_cleared`: STRUCT<
      `obd_value`: INT,
      `reason`: INT,
      `cleared_obd_value_source_lock`: STRUCT<
        `vehicle_diagnostic_bus`: INT,
        `msg_id`: DECIMAL(20, 0),
        `tx_id`: DECIMAL(20, 0),
        `obd_emitter_origin`: INT,
        `last_value_read`: BIGINT,
        `last_read_ago_ms`: DECIMAL(20, 0)
      >,
      `metadata`: STRUCT<
        `expire_after_ms`: DECIMAL(20, 0),
        `firmware_lock_version_number`: BIGINT,
        `preferred_source`: STRUCT<
          `vehicle_diagnostic_bus`: INT,
          `msg_id`: DECIMAL(20, 0),
          `tx_id`: DECIMAL(20, 0),
          `use_vehicle_diagnostic_bus`: INT,
          `use_msg_id`: INT,
          `use_tx_id`: INT
        >,
        `all_sources_seen_before_clear`: ARRAY<
          STRUCT<
            `vehicle_diagnostic_bus`: INT,
            `msg_id`: DECIMAL(20, 0),
            `tx_id`: DECIMAL(20, 0),
            `obd_emitter_origin`: INT,
            `last_value_read`: BIGINT,
            `last_read_ago_ms`: DECIMAL(20, 0)
          >
        >,
        `log_only_mode`: BOOLEAN
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
