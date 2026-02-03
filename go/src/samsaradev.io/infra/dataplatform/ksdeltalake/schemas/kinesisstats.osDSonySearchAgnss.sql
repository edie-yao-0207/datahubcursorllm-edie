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
    `sony_search_agnss`: STRUCT<
      `tcxo_bias_injected_hz`: INT,
      `temperature_at_injection_millic`: INT,
      `lle_injected_expiration_utc_sec`: DECIMAL(20, 0),
      `location_injected`: STRUCT<
        `latitude_nd`: BIGINT,
        `longitude_nd`: BIGINT,
        `altitude_mm`: INT
      >,
      `time_to_first_fix_ms`: INT,
      `tcxo_bias_at_first_fix_hz`: INT,
      `temperature_at_first_fix_millic`: INT,
      `location_at_first_fix`: STRUCT<
        `latitude_nd`: BIGINT,
        `longitude_nd`: BIGINT,
        `altitude_mm`: INT
      >,
      `accuracy_at_first_fix_mm`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
