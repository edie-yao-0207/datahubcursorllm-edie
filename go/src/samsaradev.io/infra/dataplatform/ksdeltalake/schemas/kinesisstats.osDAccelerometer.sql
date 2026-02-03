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
    `accelerometer_event`: STRUCT<
      `crash_thresh_gs`: DOUBLE,
      `max_accel_gs`: DOUBLE,
      `event_duration_ms`: BIGINT,
      `last_gps`: STRUCT<
        `latitude`: BIGINT,
        `longitude`: BIGINT,
        `speed`: BIGINT,
        `heading`: BIGINT,
        `fix`: BOOLEAN,
        `altitude`: INT,
        `hdop`: BIGINT,
        `vdop`: BIGINT,
        `ts`: BIGINT
      >,
      `recent_data`: ARRAY<
        STRUCT<
          `latitude`: BIGINT,
          `longitude`: BIGINT,
          `speed`: BIGINT,
          `heading`: BIGINT,
          `fix`: BOOLEAN,
          `altitude`: INT,
          `hdop`: BIGINT,
          `vdop`: BIGINT,
          `ts`: BIGINT
        >
      >,
      `is_debug`: BOOLEAN,
      `brake_thresh_gs`: DOUBLE,
      `event_id`: BIGINT,
      `harsh_accel_type`: INT,
      `initial_rke`: DOUBLE,
      `gateway_id`: BIGINT,
      `vision_config_proto_bytes`: BINARY,
      `vision_config_md5`: STRING,
      `harsh_accel_metadata`: STRUCT<
        `severity`: INT,
        `distracted_ms`: BIGINT,
        `distracted_calibration_ms`: BIGINT,
        `distracted_last_alert_ms`: BIGINT
      >,
      `speed_limit_metadata`: STRUCT<
        `way_id`: BIGINT,
        `speed_limit_kmph`: FLOAT,
        `match_info`: ARRAY<
          STRUCT<
            `way_id`: BIGINT,
            `lat`: DOUBLE,
            `lng`: DOUBLE,
            `speed_limit_kmph`: FLOAT,
            `distance_meters`: FLOAT,
            `unix_time_ms`: BIGINT,
            `received_at_monotonic_ms`: BIGINT,
            `speed_limit_vehicle_type`: BIGINT
          >
        >,
        `match_input`: ARRAY<
          STRUCT<
            `lat`: DOUBLE,
            `lng`: DOUBLE,
            `speed_kmph`: FLOAT,
            `heading_degrees`: FLOAT,
            `unix_time_ms`: BIGINT,
            `received_at_monotonic_ms`: BIGINT,
            `speed_source_used`: BIGINT
          >
        >,
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
          `bridge_location_server_last_modified`: BIGINT,
          `layers`: ARRAY<
            STRUCT<
              `layer_name`: INT,
              `file_path`: STRING,
              `file_on_disk_updated`: BOOLEAN,
              `file_exists`: BOOLEAN,
              `version`: STRING,
              `cache_key`: STRING,
              `server_last_modified_unix_ms`: BIGINT,
              `server_etag`: STRING,
              `file_size_bytes`: BIGINT
            >
          >
        >,
        `speeds`: ARRAY<
          STRUCT<
            `source`: BIGINT,
            `speed_kmph`: FLOAT,
            `recorded_at_ms`: BIGINT
          >
        >,
        `speed_limit_source`: INT,
        `speed_limit_layer_source`: INT
      >,
      `distracted_policy_detector_metadata`: STRUCT<
        `policy_type`: INT,
        `start_ms`: BIGINT,
        `duration_ms`: BIGINT,
        `successive_detections`: BIGINT
      >,
      `tile_rolling_stop_sign_metadata`: STRUCT<
        `node_id`: BIGINT,
        `way_id`: BIGINT,
        `lat`: DOUBLE,
        `lng`: DOUBLE,
        `lowest_speed_kmph`: FLOAT,
        `input_data`: ARRAY<
          STRUCT<
            `speed_kmph`: FLOAT,
            `lat`: DOUBLE,
            `lng`: DOUBLE,
            `utc_ms`: BIGINT,
            `way_id`: BIGINT
          >
        >,
        `match_input`: ARRAY<
          STRUCT<
            `lat`: DOUBLE,
            `lng`: DOUBLE,
            `speed_kmph`: FLOAT,
            `heading_degrees`: FLOAT,
            `unix_time_ms`: BIGINT,
            `received_at_monotonic_ms`: BIGINT,
            `speed_source_used`: BIGINT
          >
        >,
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
          `bridge_location_server_last_modified`: BIGINT,
          `layers`: ARRAY<
            STRUCT<
              `layer_name`: INT,
              `file_path`: STRING,
              `file_on_disk_updated`: BOOLEAN,
              `file_exists`: BOOLEAN,
              `version`: STRING,
              `cache_key`: STRING,
              `server_last_modified_unix_ms`: BIGINT,
              `server_etag`: STRING,
              `file_size_bytes`: BIGINT
            >
          >
        >,
        `stop_zone_enter_radius_meters`: FLOAT,
        `crossing_distance_meters`: FLOAT,
        `stop_zone_exit_radius_meters`: FLOAT,
        `distance_to_current_meters`: FLOAT,
        `speed_lookback_distance_meters`: FLOAT,
        `speed_lookback_match_input_indices`: ARRAY<BIGINT>,
        `process_result`: BIGINT,
        `bearing_degrees`: FLOAT,
        `bearing_direction`: BIGINT,
        `speeds`: ARRAY<
          STRUCT<
            `source`: BIGINT,
            `speed_kmph`: FLOAT,
            `recorded_at_ms`: BIGINT
          >
        >,
        `cv_detection_infos`: ARRAY<
          STRUCT<`detection_utc_ms`: BIGINT>
        >,
        `stop_location_way_id`: BIGINT,
        `stop_location_layer_source`: INT
      >,
      `tile_rolling_railroad_crossing_metadata`: STRUCT<
        `node_id`: BIGINT,
        `way_id`: BIGINT,
        `lat`: DOUBLE,
        `lng`: DOUBLE,
        `lowest_speed_kmph`: FLOAT,
        `input_data`: ARRAY<
          STRUCT<
            `speed_kmph`: FLOAT,
            `lat`: DOUBLE,
            `lng`: DOUBLE,
            `utc_ms`: BIGINT,
            `way_id`: BIGINT
          >
        >,
        `match_input`: ARRAY<
          STRUCT<
            `lat`: DOUBLE,
            `lng`: DOUBLE,
            `speed_kmph`: FLOAT,
            `heading_degrees`: FLOAT,
            `unix_time_ms`: BIGINT,
            `received_at_monotonic_ms`: BIGINT,
            `speed_source_used`: BIGINT
          >
        >,
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
          `bridge_location_server_last_modified`: BIGINT,
          `layers`: ARRAY<
            STRUCT<
              `layer_name`: INT,
              `file_path`: STRING,
              `file_on_disk_updated`: BOOLEAN,
              `file_exists`: BOOLEAN,
              `version`: STRING,
              `cache_key`: STRING,
              `server_last_modified_unix_ms`: BIGINT,
              `server_etag`: STRING,
              `file_size_bytes`: BIGINT
            >
          >
        >,
        `stop_zone_enter_radius_meters`: FLOAT,
        `crossing_distance_meters`: FLOAT,
        `stop_zone_exit_radius_meters`: FLOAT,
        `distance_to_current_meters`: FLOAT,
        `speed_lookback_distance_meters`: FLOAT,
        `speed_lookback_match_input_indices`: ARRAY<BIGINT>,
        `process_result`: BIGINT,
        `bearing_degrees`: FLOAT,
        `bearing_direction`: BIGINT,
        `speeds`: ARRAY<
          STRUCT<
            `source`: BIGINT,
            `speed_kmph`: FLOAT,
            `recorded_at_ms`: BIGINT
          >
        >,
        `stop_location_layer_source`: INT,
        `stop_location_way_id`: BIGINT
      >,
      `tailgating_metadata`: STRUCT<
        `start_ms`: BIGINT,
        `duration_ms`: BIGINT,
        `min_following_distance_seconds`: FLOAT
      >,
      `dashcam_seatbelt_data`: STRUCT<
        `label`: INT,
        `warning_count`: BIGINT,
        `confidence`: FLOAT,
        `gateway_id`: BIGINT,
        `event_id`: BIGINT,
        `ml_run_tag`: STRUCT<`run_id`: BIGINT>
      >,
      `dashcam_mask_data`: STRUCT<
        `label`: INT,
        `warning_count`: BIGINT,
        `confidence`: FLOAT,
        `gateway_id`: BIGINT,
        `event_id`: BIGINT
      >,
      `dashcam_driver_obstruction_data`: STRUCT<
        `label`: INT,
        `warning_count`: BIGINT,
        `confidence`: FLOAT,
        `gateway_id`: BIGINT,
        `event_id`: BIGINT
      >,
      `dashcam_passenger_detections`: STRUCT<
        `passenger_status`: INT,
        `faces_detected`: ARRAY<
          STRUCT<
            `x_frac`: FLOAT,
            `y_frac`: FLOAT,
            `w_frac`: FLOAT,
            `h_frac`: FLOAT,
            `confidence`: FLOAT
          >
        >,
        `warning_count`: BIGINT,
        `gateway_id`: BIGINT,
        `event_id`: BIGINT
      >,
      `oriented_harsh_detector_triggered`: BOOLEAN,
      `event_source`: INT,
      `ingestion_tag`: BIGINT,
      `customer_visible_ingestion_tag`: BIGINT,
      `hidden_to_customer`: BOOLEAN,
      `imu_harsh_event`: STRUCT<
        `algorithm_version`: BIGINT,
        `config`: STRUCT<
          `vehicle_type`: INT,
          `accel`: STRUCT<
            `debounce_timeout_ms`: BIGINT,
            `debounce_min_sample_count`: BIGINT,
            `cooldown_timeout_ms`: BIGINT,
            `fw_threshold_multiplier`: FLOAT,
            `thresholds_g`: STRUCT<
              `passenger`: FLOAT,
              `light`: FLOAT,
              `heavy`: FLOAT
            >,
            `customer_thresholds_g`: STRUCT<
              `passenger`: FLOAT,
              `light`: FLOAT,
              `heavy`: FLOAT
            >,
            `customer_filter_min_sample_count`: BIGINT,
            `debounce_must_be_consecutive`: BOOLEAN,
            `speed_based_acceleration_threshold_g`: FLOAT
          >,
          `brake`: STRUCT<
            `debounce_timeout_ms`: BIGINT,
            `debounce_min_sample_count`: BIGINT,
            `cooldown_timeout_ms`: BIGINT,
            `fw_threshold_multiplier`: FLOAT,
            `thresholds_g`: STRUCT<
              `passenger`: FLOAT,
              `light`: FLOAT,
              `heavy`: FLOAT
            >,
            `customer_thresholds_g`: STRUCT<
              `passenger`: FLOAT,
              `light`: FLOAT,
              `heavy`: FLOAT
            >,
            `customer_filter_min_sample_count`: BIGINT,
            `debounce_must_be_consecutive`: BOOLEAN,
            `speed_based_acceleration_threshold_g`: FLOAT
          >,
          `turn`: STRUCT<
            `debounce_timeout_ms`: BIGINT,
            `debounce_min_sample_count`: BIGINT,
            `cooldown_timeout_ms`: BIGINT,
            `fw_threshold_multiplier`: FLOAT,
            `thresholds_g`: STRUCT<
              `passenger`: FLOAT,
              `light`: FLOAT,
              `heavy`: FLOAT
            >,
            `customer_thresholds_g`: STRUCT<
              `passenger`: FLOAT,
              `light`: FLOAT,
              `heavy`: FLOAT
            >,
            `customer_filter_min_sample_count`: BIGINT,
            `debounce_must_be_consecutive`: BOOLEAN,
            `speed_based_acceleration_threshold_g`: FLOAT
          >,
          `crash`: STRUCT<
            `debounce_timeout_ms`: BIGINT,
            `debounce_min_sample_count`: BIGINT,
            `cooldown_timeout_ms`: BIGINT,
            `fw_threshold_multiplier`: FLOAT,
            `thresholds_g`: STRUCT<
              `passenger`: FLOAT,
              `light`: FLOAT,
              `heavy`: FLOAT
            >,
            `customer_thresholds_g`: STRUCT<
              `passenger`: FLOAT,
              `light`: FLOAT,
              `heavy`: FLOAT
            >,
            `customer_filter_min_sample_count`: BIGINT,
            `min_filtered_accel_mag_g`: FLOAT
          >,
          `rollover`: STRUCT<
            `window_size_sample_count`: BIGINT,
            `window_period_sample_count`: BIGINT,
            `threshold_degrees`: BIGINT,
            `max_accel_norm_g`: FLOAT,
            `max_std_dev_accel_g`: FLOAT,
            `gravity_change_within_crash_timeout_ms`: BIGINT
          >,
          `muxer`: STRUCT<
            `bundling_timeout_ms`: BIGINT,
            `finalizing_timeout_ms`: BIGINT,
            `max_event_duration_ms`: STRUCT<
              `accel`: BIGINT,
              `brake`: BIGINT,
              `turn`: BIGINT
            >
          >,
          `data_start_offset_ms`: INT,
          `data_end_offset_ms`: INT,
          `enable_accel_dc_offset_removal`: INT,
          `enable_gravity_only_crash_detection`: INT,
          `enable_algo_state_stats`: INT
        >,
        `orientation`: STRUCT<
          `gravity_vector`: STRUCT<
            `x`: FLOAT,
            `y`: FLOAT,
            `z`: FLOAT
          >,
          `yaw_angle_degrees`: FLOAT,
          `calibration_index`: BIGINT,
          `rotation_quaternion`: STRUCT<
            `w`: FLOAT,
            `v`: STRUCT<
              `x`: FLOAT,
              `y`: FLOAT,
              `z`: FLOAT
            >
          >,
          `is_loaded_from_disk`: BOOLEAN,
          `lock_valid`: INT,
          `algorithm_version`: BIGINT
        >,
        `secondary_events`: ARRAY<
          STRUCT<
            `start_ms`: BIGINT,
            `duration_ms`: BIGINT,
            `max_accel_g`: FLOAT,
            `type`: INT,
            `triggered_audio_alert`: BOOLEAN,
            `triggered_customer_threshold`: BOOLEAN,
            `num_samples_above_customer_threshold`: BIGINT,
            `max_customer_threshold_passed_g`: FLOAT,
            `triggered_customer_threshold_based_on_imu`: BOOLEAN
          >
        >,
        `triggered_audio_alert`: BOOLEAN,
        `triggered_customer_threshold`: BOOLEAN,
        `num_samples_above_customer_threshold`: BIGINT,
        `gravity_only_event`: BOOLEAN,
        `gravity_vector`: STRUCT<
          `x`: FLOAT,
          `y`: FLOAT,
          `z`: FLOAT
        >,
        `max_customer_threshold_passed_g`: FLOAT,
        `triggered_customer_threshold_based_on_imu`: BOOLEAN,
        `reboot_to_readonly_event`: BOOLEAN
      >,
      `ml_run_tag`: STRUCT<`run_id`: BIGINT>,
      `ldw_metadata`: STRUCT<
        `ldw_model_output`: STRUCT<
          `class`: INT,
          `confidence`: FLOAT
        >
      >,
      `drowsiness_metadata`: STRUCT<
        `severity`: INT,
        `aggregation_window_ms`: BIGINT
      >,
      `sign_detection_metadata`: STRUCT<
        `sign_type`: INT,
        `confidence`: FLOAT,
        `bbox`: STRUCT<
          `x_min_pct`: FLOAT,
          `y_min_pct`: FLOAT,
          `x_max_pct`: FLOAT,
          `y_max_pct`: FLOAT
        >,
        `detection_start_time_ms`: BIGINT,
        `harsh_event_debounce_time_left_ms`: BIGINT,
        `object_class`: BIGINT,
        `output_tensor_index`: BIGINT,
        `is_shadow`: BOOLEAN,
        `skip_bus_message`: BOOLEAN
      >,
      `vulnerable_road_user_collision_warning_metadata`: STRUCT<
        `media_inputs`: ARRAY<INT>
      >,
      `is_low_severity_hidden_event`: BOOLEAN,
      `low_severity_hidden_event_type`: INT,
      `traffic_light_violation_metadata`: STRUCT<
        `node_id`: BIGINT,
        `way_id`: BIGINT,
        `lat`: DOUBLE,
        `lng`: DOUBLE,
        `lowest_speed_kmph`: FLOAT,
        `input_data`: ARRAY<
          STRUCT<
            `speed_kmph`: FLOAT,
            `lat`: DOUBLE,
            `lng`: DOUBLE,
            `utc_ms`: BIGINT,
            `way_id`: BIGINT
          >
        >,
        `match_input`: ARRAY<
          STRUCT<
            `lat`: DOUBLE,
            `lng`: DOUBLE,
            `speed_kmph`: FLOAT,
            `heading_degrees`: FLOAT,
            `unix_time_ms`: BIGINT,
            `received_at_monotonic_ms`: BIGINT,
            `speed_source_used`: BIGINT
          >
        >,
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
          `bridge_location_server_last_modified`: BIGINT,
          `layers`: ARRAY<
            STRUCT<
              `layer_name`: INT,
              `file_path`: STRING,
              `file_on_disk_updated`: BOOLEAN,
              `file_exists`: BOOLEAN,
              `version`: STRING,
              `cache_key`: STRING,
              `server_last_modified_unix_ms`: BIGINT,
              `server_etag`: STRING,
              `file_size_bytes`: BIGINT
            >
          >
        >,
        `stop_zone_enter_radius_meters`: FLOAT,
        `crossing_distance_meters`: FLOAT,
        `stop_zone_exit_radius_meters`: FLOAT,
        `distance_to_current_meters`: FLOAT,
        `speed_lookback_distance_meters`: FLOAT,
        `speed_lookback_match_input_indices`: ARRAY<BIGINT>,
        `process_result`: BIGINT,
        `bearing_degrees`: FLOAT,
        `bearing_direction`: BIGINT,
        `speeds`: ARRAY<
          STRUCT<
            `source`: BIGINT,
            `speed_kmph`: FLOAT,
            `recorded_at_ms`: BIGINT
          >
        >,
        `cv_detection_infos`: ARRAY<
          STRUCT<
            `detection_utc_ms`: BIGINT,
            `detection_monotonic_ms`: BIGINT,
            `traffic_light_state`: INT
          >
        >,
        `stop_location_way_id`: BIGINT,
        `stop_location_layer_source`: INT,
        `cv_detection_parameters`: STRUCT<
          `detection_min_age_ms`: BIGINT,
          `detection_max_age_ms`: BIGINT,
          `min_red_light_percent`: FLOAT
        >,
        `first_path_index_after_crossing`: BIGINT
      >,
      `detection_identification_info`: STRUCT<`detection_id`: BIGINT>,
      `traffic_light_detection_metadata`: STRUCT<
        `traffic_light_detections`: ARRAY<
          STRUCT<
            `confidence`: FLOAT,
            `bbox`: STRUCT<
              `x_min_pct`: FLOAT,
              `y_min_pct`: FLOAT,
              `x_max_pct`: FLOAT,
              `y_max_pct`: FLOAT
            >,
            `detection_start_time_ms`: BIGINT,
            `traffic_light_class`: BIGINT
          >
        >,
        `governing_traffic_light`: STRUCT<
          `identified`: BOOLEAN,
          `traffic_light_index`: BIGINT,
          `detection_start_time_ms`: BIGINT,
          `confidence_threshold`: FLOAT
        >
      >,
      `vehicle_in_blind_spot_warning_metadata`: STRUCT<`media_input`: INT>,
      `detection_metadata`: STRUCT<
        `detection_mode`: INT,
        `ml_app_profile_params`: STRUCT<
          `profile_name`: STRING,
          `cohort_name`: STRING,
          `feature_name`: STRING
        >
      >,
      `rear_collision_warning_metadata`: STRUCT<`media_input`: INT>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
