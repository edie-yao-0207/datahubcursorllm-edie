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
    `edge_speed_limit`: STRUCT<
      `speed_limit_kmph`: FLOAT,
      `way_id`: BIGINT,
      `has_edge_speed_limit`: BOOLEAN,
      `lat`: DOUBLE,
      `lng`: DOUBLE,
      `gps_received_at_utc_ms`: BIGINT,
      `speed_limit_vehicle_type`: BIGINT,
      `current_tile_info`: STRUCT<
        `slippy_tile`: STRING,
        `version`: STRING,
        `server_last_modified`: BIGINT,
        `tiles_cache_key`: STRING,
        `overrides_cache_key`: STRING,
        `overrides_server_last_modified`: BIGINT,
        `did_bridge_location_lookup`: BOOLEAN,
        `bridge_location_version`: STRING,
        `bridge_location_cache_key`: STRING,
        `bridge_location_server_last_modified`: BIGINT
      >,
      `error_message`: STRING,
      `speed_limit_source`: INT,
      `speed_limit_layer_source`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
