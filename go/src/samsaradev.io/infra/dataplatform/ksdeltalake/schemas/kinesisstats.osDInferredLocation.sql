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
    `inferred_location`: STRUCT<
      `latitude_degrees`: DOUBLE,
      `longitude_degrees`: DOUBLE,
      `altitude_meters`: DOUBLE,
      `accuracy_millimeters`: BIGINT,
      `inference_processed_at_ms`: BIGINT,
      `timestamps_of_locations_used_for_inference`: ARRAY<BIGINT>,
      `location_source`: STRUCT<
        `org_id`: BIGINT,
        `device_id`: BIGINT,
        `in_same_org`: BOOLEAN,
        `object_type`: INT
      >,
      `peripheral_protocol_version`: INT,
      `service_uuid`: INT,
      `tx_power_is_calibrated`: BOOLEAN,
      `peripheral_product_id`: BIGINT,
      `central_product_id`: BIGINT,
      `observation_period_start_time_ms`: BIGINT,
      `observation_series`: STRUCT<
        `incremental_observed_at_offset_ds`: ARRAY<BIGINT>,
        `incremental_rssi_dbm`: ARRAY<INT>,
        `incremental_tx_power_dbm`: ARRAY<INT>
      >,
      `locations_during_observation_period`: ARRAY<
        STRUCT<
          `time`: BIGINT,
          `has_fix`: BOOLEAN,
          `latitude`: DOUBLE,
          `longitude`: DOUBLE,
          `altitude_meters`: DOUBLE,
          `accuracy_millimeters`: BIGINT,
          `gps_fix_timestamp_utc_ms`: DECIMAL(20, 0),
          `gps_speed_meters_per_second`: DOUBLE,
          `received_at_ms`: BIGINT,
          `is_spoofed`: BOOLEAN,
          `is_fixed`: BOOLEAN,
          `hdop`: DOUBLE,
          `vdop`: DOUBLE
        >
      >,
      `time_since_last_motion`: BIGINT,
      `source_network`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
