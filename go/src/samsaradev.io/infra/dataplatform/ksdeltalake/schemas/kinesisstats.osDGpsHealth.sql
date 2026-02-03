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
    `gps_health`: STRUCT<
      `fix_data`: STRUCT<`duration_ms`: BIGINT>,
      `no_fix_data`: STRUCT<`duration_ms`: BIGINT>,
      `periods`: ARRAY<
        STRUCT<
          `gps_state`: INT,
          `duration_ms`: BIGINT
        >
      >,
      `fix_satellites_mean_snr_dbhz`: FLOAT,
      `systems_seen`: ARRAY<
        STRUCT<
          `gnss_system_name`: INT,
          `total_satellite_count`: BIGINT,
          `tracked_satellite_count`: BIGINT
        >
      >,
      `fix_constellations`: ARRAY<INT>,
      `speed_valid_duration_ms`: BIGINT,
      `fix_satellites_linear_mean_snr_dbhz`: DOUBLE,
      `visible_satellites_linear_mean_snr_dbhz`: DOUBLE
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
