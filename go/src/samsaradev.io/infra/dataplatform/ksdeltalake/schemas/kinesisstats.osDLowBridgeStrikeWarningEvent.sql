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
    `low_bridge_strike_warning_event`: STRUCT<
      `event_id`: DECIMAL(20, 0),
      `config`: STRUCT<
        `enabled`: INT,
        `vehicle_height_meters`: FLOAT,
        `height_buffer_meters`: FLOAT,
        `ways_away_trigger`: STRUCT<
          `enabled`: INT,
          `ways_away_from_bridge_way_threshold`: BIGINT
        >,
        `distance_trigger`: STRUCT<
          `enabled`: INT,
          `distance_to_bridge_node_threshold_meters`: FLOAT
        >,
        `heading_check`: STRUCT<
          `enabled`: INT,
          `heading_margin_degrees`: FLOAT
        >,
        `clear_processed_bridge_node_distance_meters`: FLOAT,
        `require_accel_movement_enabled`: INT,
        `audio_alerts_in_shadow_mode_enabled`: INT,
        `processed_bridge_location_upload_enabled`: INT,
        `max_detection_distance_meters`: FLOAT,
        `tile_layers`: ARRAY<INT>,
        `tile_layer_overrides`: ARRAY<INT>
      >,
      `ways_away_threshold_triggered`: BOOLEAN,
      `distance_threshold_triggered`: BOOLEAN,
      `height_threshold_triggered`: BOOLEAN,
      `hidden_to_customer`: BOOLEAN,
      `bridge_way_id`: DECIMAL(20, 0),
      `bridge_height_meters`: FLOAT,
      `parent_node`: STRUCT<
        `node_id`: DECIMAL(20, 0),
        `lat_degrees`: DOUBLE,
        `lng_degrees`: DOUBLE
      >,
      `bridge_node`: STRUCT<
        `node_id`: DECIMAL(20, 0),
        `lat_degrees`: DOUBLE,
        `lng_degrees`: DOUBLE
      >,
      `ways_away_from_bridge_way`: BIGINT,
      `distance_to_bridge_node_meters`: FLOAT,
      `distance_to_parent_node_meters`: FLOAT,
      `parent_node_to_bridge_node_distance_meters`: FLOAT,
      `current_location_to_parent_node_heading_degrees`: FLOAT,
      `current_osrm_match`: STRUCT<
        `way_name`: STRING,
        `way_id`: DECIMAL(20, 0),
        `lat_degrees`: DOUBLE,
        `lng_degrees`: DOUBLE,
        `gps_utc_ms`: BIGINT
      >,
      `current_gps`: STRUCT<
        `complete_fix`: BOOLEAN,
        `lat_degrees`: DOUBLE,
        `lng_degrees`: DOUBLE,
        `altitude_milli_m`: INT,
        `altitude_correction`: INT,
        `speed_milli_knot`: BIGINT,
        `heading_milli_deg`: BIGINT,
        `hdop_thousandths`: BIGINT,
        `vdop_thousandths`: BIGINT,
        `utc_ms`: BIGINT,
        `accuracy_valid`: BOOLEAN,
        `accuracy_milli_m`: BIGINT
      >,
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
      >,
      `bridge_group_id`: DECIMAL(20, 0),
      `bridge_location_layer_source`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
