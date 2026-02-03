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
    `current_tile_info`: STRUCT<
      `slippy_tile`: STRING,
      `version`: STRING,
      `server_last_modified`: DECIMAL(20, 0),
      `tiles_cache_key`: STRING,
      `overrides_cache_key`: STRING,
      `overrides_server_last_modified`: DECIMAL(20, 0),
      `did_bridge_location_lookup`: BOOLEAN,
      `bridge_location_version`: STRING,
      `bridge_location_cache_key`: STRING,
      `bridge_location_server_last_modified`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
