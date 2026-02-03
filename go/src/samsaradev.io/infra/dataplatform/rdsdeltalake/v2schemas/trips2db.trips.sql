`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`device_id` BIGINT,
`driver_id` BIGINT,
`version` INT,
`start_ms` BIGINT,
`end_ms` BIGINT,
`proto` STRUCT<
  `start`: STRUCT<
    `time`: BIGINT,
    `latitude`: DOUBLE,
    `longitude`: DOUBLE,
    `address`: STRUCT<
      `id`: BIGINT,
      `name`: STRING,
      `address`: STRING,
      `latitude`: DOUBLE,
      `longitude`: DOUBLE
    >,
    `place`: STRUCT<
      `street`: STRING,
      `city`: STRING,
      `state`: STRING,
      `house_number`: STRING,
      `poi`: STRING,
      `neighborhood`: STRING,
      `postcode`: STRING,
      `country`: STRING
    >,
    `heading_degrees`: DOUBLE,
    `accuracy_millimeters`: BIGINT
  >,
  `end`: STRUCT<
    `time`: BIGINT,
    `latitude`: DOUBLE,
    `longitude`: DOUBLE,
    `address`: STRUCT<
      `id`: BIGINT,
      `name`: STRING,
      `address`: STRING,
      `latitude`: DOUBLE,
      `longitude`: DOUBLE
    >,
    `place`: STRUCT<
      `street`: STRING,
      `city`: STRING,
      `state`: STRING,
      `house_number`: STRING,
      `poi`: STRING,
      `neighborhood`: STRING,
      `postcode`: STRING,
      `country`: STRING
    >,
    `heading_degrees`: DOUBLE,
    `accuracy_millimeters`: BIGINT
  >,
  `distance_meters`: DOUBLE,
  `toll_meters`: DOUBLE,
  `engine_on_ms`: BIGINT,
  `engine_idle_ms`: BIGINT,
  `driver_id`: BIGINT,
  `max_speed_kmph`: DOUBLE,
  `max_speed_at`: BIGINT,
  `over_speed_limit_ms`: BIGINT,
  `harsh_accel_count`: BIGINT,
  `harsh_brake_count`: BIGINT,
  `harsh_corner_count`: BIGINT,
  `bounds`: STRUCT<
    `south_west_latitude`: DOUBLE,
    `south_west_longitude`: DOUBLE,
    `north_east_latitude`: DOUBLE,
    `north_east_longitude`: DOUBLE
  >,
  `has_id_card`: BOOLEAN,
  `id_card`: BIGINT,
  `fuel_consumed_ml`: DOUBLE,
  `driver_source_id`: BIGINT,
  `harsh_events`: ARRAY<
    STRUCT<
      `start`: STRUCT<
        `latitude`: BIGINT,
        `longitude`: BIGINT,
        `speed_milliknots`: BIGINT,
        `at_ms`: BIGINT
      >,
      `stop`: STRUCT<
        `latitude`: BIGINT,
        `longitude`: BIGINT,
        `speed_milliknots`: BIGINT,
        `at_ms`: BIGINT
      >,
      `duration_ms`: BIGINT,
      `at_ms`: BIGINT,
      `accel_type`: INT,
      `max_accel_gs`: DOUBLE,
      `brake_thresh_gs`: DOUBLE,
      `device_id`: BIGINT,
      `target_gateway_id`: BIGINT,
      `event_id`: BIGINT,
      `crash_thresh_gs`: DOUBLE,
      `reviewed_by_samsara_at_ms`: BIGINT,
      `ingestion_tag`: DECIMAL(20, 0),
      `customer_visible_ingestion_tag`: DECIMAL(20, 0),
      `hidden_to_customer`: BOOLEAN
    >
  >,
  `start_odometer`: BIGINT,
  `end_odometer`: BIGINT,
  `previous_trip_end_odometer`: BIGINT,
  `start_odometer_time_ms`: BIGINT,
  `end_odometer_time_ms`: BIGINT,
  `hos_unassigned`: ARRAY<
    STRUCT<
      `log_ms`: BIGINT,
      `end_ms`: BIGINT,
      `status`: BIGINT,
      `lat`: DOUBLE,
      `lng`: DOUBLE,
      `loc_city`: STRING,
      `loc_state`: STRING,
      `log_type`: BIGINT,
      `org_id`: BIGINT,
      `group_id`: BIGINT,
      `vehicle_id`: BIGINT,
      `driver_id`: BIGINT,
      `notes`: STRING,
      `loc_needs_fixup`: BOOLEAN,
      `created_at_ms`: BIGINT,
      `updated_at_ms`: BIGINT,
      `server_created`: BOOLEAN
    >
  >,
  `hos_unassigned_ms`: BIGINT,
  `trip_distance`: STRUCT<
    `distance_meters`: DOUBLE,
    `toll_meters`: DOUBLE
  >,
  `trip_speeding`: STRUCT<
    `max_speed_kmph`: DOUBLE,
    `max_speed_at`: BIGINT,
    `over_speed_limit_ms`: BIGINT
  >,
  `trip_static_driver`: STRUCT<`driver_id`: BIGINT>,
  `trip_bounds`: STRUCT<
    `bounds`: STRUCT<
      `south_west_latitude`: DOUBLE,
      `south_west_longitude`: DOUBLE,
      `north_east_latitude`: DOUBLE,
      `north_east_longitude`: DOUBLE
    >
  >,
  `trip_engine`: STRUCT<
    `engine_on_ms`: BIGINT,
    `engine_idle_ms`: BIGINT
  >,
  `trip_id_card`: STRUCT<
    `has_id_card`: BOOLEAN,
    `id_card`: BIGINT,
    `driver_id`: BIGINT
  >,
  `trip_fuel`: STRUCT<`fuel_consumed_ml`: DOUBLE>,
  `trip_harsh_events`: STRUCT<
    `harsh_events`: ARRAY<
      STRUCT<
        `start`: STRUCT<
          `latitude`: BIGINT,
          `longitude`: BIGINT,
          `speed_milliknots`: BIGINT,
          `at_ms`: BIGINT
        >,
        `stop`: STRUCT<
          `latitude`: BIGINT,
          `longitude`: BIGINT,
          `speed_milliknots`: BIGINT,
          `at_ms`: BIGINT
        >,
        `duration_ms`: BIGINT,
        `at_ms`: BIGINT,
        `accel_type`: INT,
        `max_accel_gs`: DOUBLE,
        `brake_thresh_gs`: DOUBLE,
        `device_id`: BIGINT,
        `target_gateway_id`: BIGINT,
        `event_id`: BIGINT,
        `crash_thresh_gs`: DOUBLE,
        `reviewed_by_samsara_at_ms`: BIGINT,
        `ingestion_tag`: DECIMAL(20, 0),
        `customer_visible_ingestion_tag`: DECIMAL(20, 0),
        `hidden_to_customer`: BOOLEAN
      >
    >,
    `harsh_accel_count`: BIGINT,
    `harsh_brake_count`: BIGINT,
    `harsh_corner_count`: BIGINT,
    `rolling_stop_count`: BIGINT,
    `other_harsh_event_count`: BIGINT
  >,
  `trip_odometers`: STRUCT<
    `start_odometer`: BIGINT,
    `start_odometer_time_ms`: BIGINT,
    `end_odometer`: BIGINT,
    `end_odometer_time_ms`: BIGINT,
    `previous_trip_end_odometer`: BIGINT,
    `previous_trip_end_odometer_time_ms`: BIGINT,
    `start_gps_odometer`: BIGINT,
    `end_gps_odometer`: BIGINT
  >,
  `trip_vehicle_assignment`: STRUCT<`driver_id`: BIGINT>,
  `built_at`: BIGINT,
  `seatbelt_unbuckled_ms`: BIGINT,
  `seatbelt_reporting_present`: BOOLEAN,
  `ongoing`: BOOLEAN,
  `prev_end`: STRUCT<
    `time`: BIGINT,
    `latitude`: DOUBLE,
    `longitude`: DOUBLE,
    `address`: STRUCT<
      `id`: BIGINT,
      `name`: STRING,
      `address`: STRING,
      `latitude`: DOUBLE,
      `longitude`: DOUBLE
    >,
    `place`: STRUCT<
      `street`: STRING,
      `city`: STRING,
      `state`: STRING,
      `house_number`: STRING,
      `poi`: STRING,
      `neighborhood`: STRING,
      `postcode`: STRING,
      `country`: STRING
    >,
    `heading_degrees`: DOUBLE,
    `accuracy_millimeters`: BIGINT
  >,
  `trip_seatbelt`: STRUCT<
    `unbuckled_ms`: BIGINT,
    `reporting_present`: BOOLEAN
  >,
  `driver_face_id`: STRUCT<
    `org_id`: BIGINT,
    `face_id`: STRING,
    `device_id`: BIGINT,
    `asset_ms`: BIGINT,
    `driver_id`: BIGINT,
    `match_similarity_score`: FLOAT
  >,
  `trip_tachograph_vehicle_assignment`: STRUCT<
    `driver_id`: BIGINT,
    `is_primary`: BOOLEAN,
    `card_number`: STRING
  >,
  `trip_manual_driver_assignment`: STRUCT<
    `driver_id`: BIGINT,
    `is_present`: BOOLEAN
  >,
  `trip_rfid_card_driver_assignment`: STRUCT<`driver_id`: BIGINT>,
  `trip_speeding_mph`: STRUCT<
    `unit`: INT,
    `not_speeding_ms`: BIGINT,
    `light_speeding_ms`: BIGINT,
    `moderate_speeding_ms`: BIGINT,
    `heavy_speeding_ms`: BIGINT,
    `severe_speeding_ms`: BIGINT,
    `version`: INT
  >,
  `trip_speeding_kph`: STRUCT<
    `unit`: INT,
    `not_speeding_ms`: BIGINT,
    `light_speeding_ms`: BIGINT,
    `moderate_speeding_ms`: BIGINT,
    `heavy_speeding_ms`: BIGINT,
    `severe_speeding_ms`: BIGINT,
    `version`: INT
  >,
  `trip_speeding_percent`: STRUCT<
    `unit`: INT,
    `not_speeding_ms`: BIGINT,
    `light_speeding_ms`: BIGINT,
    `moderate_speeding_ms`: BIGINT,
    `heavy_speeding_ms`: BIGINT,
    `severe_speeding_ms`: BIGINT,
    `version`: INT
  >,
  `trip_settings`: STRUCT<
    `speed_threshold`: DOUBLE,
    `min_datapoint_count`: BIGINT,
    `start_speed_threshold`: DOUBLE,
    `end_speed_threshold`: DOUBLE,
    `ignore_administrative_boundaries`: BOOLEAN
  >,
  `trip_trailer_assignment`: STRUCT<`driver_id`: BIGINT>,
  `pre_release_trip_speeding_mph`: STRUCT<
    `unit`: INT,
    `not_speeding_ms`: BIGINT,
    `light_speeding_ms`: BIGINT,
    `moderate_speeding_ms`: BIGINT,
    `heavy_speeding_ms`: BIGINT,
    `severe_speeding_ms`: BIGINT,
    `version`: INT
  >,
  `pre_release_trip_speeding_kph`: STRUCT<
    `unit`: INT,
    `not_speeding_ms`: BIGINT,
    `light_speeding_ms`: BIGINT,
    `moderate_speeding_ms`: BIGINT,
    `heavy_speeding_ms`: BIGINT,
    `severe_speeding_ms`: BIGINT,
    `version`: INT
  >,
  `pre_release_trip_speeding_percent`: STRUCT<
    `unit`: INT,
    `not_speeding_ms`: BIGINT,
    `light_speeding_ms`: BIGINT,
    `moderate_speeding_ms`: BIGINT,
    `heavy_speeding_ms`: BIGINT,
    `severe_speeding_ms`: BIGINT,
    `version`: INT
  >,
  `device_id`: BIGINT,
  `trip_external_assignment`: STRUCT<
    `driver_id`: BIGINT,
    `metadata_id`: BINARY
  >,
  `org_id`: BIGINT,
  `trip_speeding_count_mph`: STRUCT<
    `unit`: INT,
    `not_speeding_count`: BIGINT,
    `light_speeding_count`: BIGINT,
    `moderate_speeding_count`: BIGINT,
    `heavy_speeding_count`: BIGINT,
    `severe_speeding_count`: BIGINT,
    `version`: INT
  >,
  `trip_speeding_count_kph`: STRUCT<
    `unit`: INT,
    `not_speeding_count`: BIGINT,
    `light_speeding_count`: BIGINT,
    `moderate_speeding_count`: BIGINT,
    `heavy_speeding_count`: BIGINT,
    `severe_speeding_count`: BIGINT,
    `version`: INT
  >,
  `pre_release_driver_assignment`: STRUCT<
    `driver_id`: BIGINT,
    `driver_source_id`: BIGINT
  >,
  `trip_speeding_custom`: STRUCT<
    `unit`: INT,
    `not_speeding_ms`: BIGINT,
    `light_speeding_ms`: BIGINT,
    `moderate_speeding_ms`: BIGINT,
    `heavy_speeding_ms`: BIGINT,
    `severe_speeding_ms`: BIGINT,
    `version`: INT
  >,
  `trip_speeding_count_custom`: STRUCT<
    `unit`: INT,
    `not_speeding_count`: BIGINT,
    `light_speeding_count`: BIGINT,
    `moderate_speeding_count`: BIGINT,
    `heavy_speeding_count`: BIGINT,
    `severe_speeding_count`: BIGINT,
    `version`: INT
  >,
  `trip_q_r_code_assignment`: STRUCT<`driver_id`: BIGINT>,
  `trip_end_reason`: INT,
  `trip_type`: INT
>,
`_raw_proto` STRING,
`created_at` BIGINT,
`updated_at` BIGINT,
`end_reason` BYTE,
`end_reason_updated_at` BIGINT,
`date` STRING
