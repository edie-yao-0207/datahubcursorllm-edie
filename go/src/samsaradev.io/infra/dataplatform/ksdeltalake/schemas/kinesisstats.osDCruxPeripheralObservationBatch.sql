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
    `peripheral_observation_batch`: STRUCT<
      `peripheral_observations`: ARRAY<
        STRUCT<
          `org_id`: BIGINT,
          `hardware_id`: BIGINT,
          `product_id`: BIGINT,
          `observation_series`: STRUCT<
            `incremental_observed_at_offset_ds`: ARRAY<BIGINT>,
            `incremental_rssi_dbm`: ARRAY<INT>,
            `incremental_tx_power_dbm`: ARRAY<INT>
          >,
          `protocol_version`: BIGINT,
          `tx_power_is_calibrated`: BOOLEAN,
          `service_uuid`: INT,
          `central_product_id`: BIGINT,
          `time_since_last_motion`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
