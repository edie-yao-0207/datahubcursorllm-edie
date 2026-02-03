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
    `crux_debug`: STRUCT<
      `protocol_version`: BIGINT,
      `flags`: BIGINT,
      `time_since_connection`: BIGINT,
      `tx_power_dbm`: INT,
      `fw_version`: BIGINT,
      `battery_level_mv`: BIGINT,
      `battery_level_counts`: BIGINT,
      `temperature_c`: FLOAT,
      `temperature_counts`: BIGINT,
      `error`: BIGINT,
      `reset_reason`: INT,
      `num_reboots`: BIGINT,
      `mfg_specific_data`: BINARY,
      `observation_series`: STRUCT<
        `incremental_observed_at_offset_ds`: ARRAY<BIGINT>,
        `incremental_rssi_dbm`: ARRAY<INT>,
        `incremental_tx_power_dbm`: ARRAY<INT>
      >,
      `security_data`: STRUCT<
        `counter`: BIGINT,
        `signature`: BINARY,
        `nonce_bytes`: BINARY
      >,
      `central_id`: DECIMAL(20, 0),
      `battery_level_invalid`: BOOLEAN,
      `temperature_invalid`: BOOLEAN,
      `reset_reason_invalid`: BOOLEAN,
      `num_reboots_invalid`: BOOLEAN,
      `error_invalid`: BOOLEAN,
      `fw_version_invalid`: BOOLEAN,
      `service_uuid`: BIGINT,
      `num_times_observed_during_batch_period`: BIGINT,
      `time_since_last_motion`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
