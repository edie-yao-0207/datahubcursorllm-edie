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
    `ble_proxied_advertisement`: STRUCT<
      `peripherals`: ARRAY<
        STRUCT<
          `uuid`: INT,
          `mac_observed`: DECIMAL(20, 0),
          `num_times_observed_during_batch_period`: BIGINT,
          `last_adv_packet_data`: BINARY,
          `tx_power_is_invalid`: BOOLEAN,
          `observation_series`: STRUCT<
            `incremental_observed_at_offset_ds`: ARRAY<BIGINT>,
            `incremental_rssi_dbm`: ARRAY<INT>,
            `incremental_tx_power_dbm`: ARRAY<INT>
          >,
          `third_party_integration_id`: INT
        >
      >,
      `batch_duration_sec`: BIGINT,
      `mobile_visibility_state`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
