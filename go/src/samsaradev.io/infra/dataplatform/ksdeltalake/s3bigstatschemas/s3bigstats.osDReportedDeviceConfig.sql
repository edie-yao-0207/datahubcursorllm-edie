`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`_s3_big_stat_file_created_at_ms` BIGINT,
`s3_proto_value` STRUCT<
  `reported_device_config`: STRUCT<
    `device_config`: STRUCT<
      `cfg_ver`: BIGINT,
      `name`: STRING,
      `whitelisted_widget`: ARRAY<
        STRUCT<
          `widget_id`: BIGINT,
          `widget_name`: STRING,
          `widget_ltk`: ARRAY<
            STRUCT<
              `key`: BINARY,
              `diversifier`: BIGINT
            >
          >,
          `product_shortname`: STRING,
          `ava_config`: STRUCT<
            `output_enabled`: BOOLEAN,
            `polling_interval_sec`: BIGINT
          >
        >
      >,
      `ssids`: ARRAY<
        STRUCT<
          `name`: STRING,
          `encryption_type`: INT,
          `wpa_passphrase`: STRING,
          `wpa_enterprise_username`: STRING,
          `wpa_enterprise_password`: STRING
        >
      >,
      `group_id`: BIGINT,
      `no_hub_reboot_mins`: INT,
      `ble_connection_interval_min_ms`: INT,
      `ble_connection_interval_max_ms`: INT,
      `ble_slave_latency`: INT,
      `ble_supervisory_timeout_ms`: INT,
      `passenger_obd_command_config`: ARRAY<
        STRUCT<
          `command`: INT,
          `num_bytes`: INT,
          `record_status`: INT,
          `record_period_ms`: INT
        >
      >,
      `j1939_obd_command_config`: ARRAY<
        STRUCT<
          `command`: INT,
          `num_bytes`: INT,
          `record_status`: INT,
          `record_period_ms`: INT
        >
      >,
      `obd_config`: STRUCT<
        `FaultTimeoutMs`: BIGINT,
        `FaultMinAgeMs`: BIGINT,
        `vin_specific`: STRUCT<
          `vin`: STRING,
          `cmds`: ARRAY<
            STRUCT<
              `data_type`: INT,
              `module_address`: INT,
              `command`: INT,
              `bit_shift`: BIGINT,
              `command_len`: INT,
              `parse_parameters`: STRUCT<
                `multiplier`: DOUBLE,
                `offset_bytes`: BIGINT,
                `length_bytes`: BIGINT
              >,
              `response_id`: BIGINT,
              `accept_any_response_id`: BOOLEAN
            >
          >,
          `cmds_with_bit_shift`: ARRAY<
            STRUCT<
              `data_type`: INT,
              `module_address`: INT,
              `command`: INT,
              `bit_shift`: BIGINT,
              `command_len`: INT,
              `parse_parameters`: STRUCT<
                `multiplier`: DOUBLE,
                `offset_bytes`: BIGINT,
                `length_bytes`: BIGINT
              >,
              `response_id`: BIGINT,
              `accept_any_response_id`: BOOLEAN
            >
          >,
          `passive_cmds`: ARRAY<
            STRUCT<
              `data_type`: INT,
              `can_id`: BIGINT,
              `byte_offset`: BIGINT,
              `byte_len`: BIGINT
            >
          >,
          `diagnostic_lock_configs`: ARRAY<
            STRUCT<
              `obd_values_high_to_low_priority`: ARRAY<INT>,
              `disable`: BOOLEAN,
              `disable_expire`: BOOLEAN,
              `delay_set_ms`: DECIMAL(20, 0),
              `expire_ms`: DECIMAL(20, 0),
              `preferred_tx_id`: DECIMAL(20, 0)
            >
          >
        >,
        `skip_passenger_command_check`: BOOLEAN,
        `engine_tracker_update_movement`: BOOLEAN,
        `dump`: STRUCT<
          `pgns`: ARRAY<INT>,
          `skip_repeats`: BOOLEAN,
          `always_dump_packets`: BOOLEAN,
          `disable_file_dump`: BOOLEAN,
          `enable_bus_publishing`: INT,
          `max_packets`: DECIMAL(20, 0),
          `low_res_timestamping_enabled`: INT,
          `low_res_timestamping_resolution_ms`: BIGINT
        >,
        `reduce_speed_threshold_mk`: BIGINT,
        `engine_gauge_update_ms`: BIGINT,
        `probe_type`: INT,
        `disable_cable_id`: BOOLEAN,
        `passenger_wait_for_movement`: BOOLEAN,
        `j1939_auto_detect_behavior`: INT,
        `disable_j1939_active_on_elm327`: BOOLEAN,
        `disable_j1939_low_res_odometer`: BOOLEAN,
        `pgn_status_report_delay_ms`: BIGINT,
        `allow_all_can11_speed_ecus`: BOOLEAN,
        `j1939_auto_detect_hysteresis_threshold`: INT,
        `j1939_auto_detect_behavior_with_hysteresis`: INT,
        `min_idle_duration_ms`: BIGINT,
        `smog_check_tracker_read_period_ms`: BIGINT,
        `enable_passenger_can_filter`: BOOLEAN,
        `j1939_pin_auto_detect_result`: BOOLEAN,
        `odometer_log_period_ms`: BIGINT,
        `engine_off_debounce_duration_ms`: BIGINT,
        `disable_maf_on_diesel_vehicles`: BOOLEAN,
        `enable_vds_harsh_events`: BOOLEAN,
        `disable_runtime_based_engine_state`: BOOLEAN,
        `use_vcan_bus_for_j1939_and_passenger`: BOOLEAN,
        `fuel_type_override`: INT,
        `j1939_ccvs_speed_source`: INT,
        `disable_engine_tracker_object_stat_flags`: BOOLEAN,
        `raw_vins_log_interval_ms`: DECIMAL(20, 0),
        `vin_specific_v2`: STRUCT<
          `active_commands`: ARRAY<
            STRUCT<
              `emitter_config`: ARRAY<
                STRUCT<
                  `obd_value`: INT,
                  `formula`: STRING,
                  `validator`: STRING,
                  `bit_start`: BIGINT,
                  `bit_length`: BIGINT,
                  `endianness`: INT,
                  `sign`: INT,
                  `emitter_offset`: DOUBLE,
                  `emitter_scale`: DOUBLE,
                  `emitter_min`: DOUBLE,
                  `emitter_max`: DOUBLE,
                  `emitter_unit_conversion_scale`: DOUBLE,
                  `obd_protocol`: INT,
                  `signal_state_encoding`: STRUCT<
                    `state_encoding_enabled`: INT,
                    `mappings`: ARRAY<
                      STRUCT<
                        `input_signal_state`: BIGINT,
                        `output_signal_state`: BIGINT
                      >
                    >,
                    `default_enabled`: INT,
                    `default_value`: BIGINT
                  >
                >
              >,
              `module_address`: BIGINT,
              `command`: BINARY,
              `response_address`: BIGINT,
              `read_period_ms`: DECIMAL(20, 0),
              `vehicle_diagnostic_input`: INT,
              `vehicle_diagnostic_bus`: INT
            >
          >,
          `passive_commands`: ARRAY<
            STRUCT<
              `emitter_config`: ARRAY<
                STRUCT<
                  `obd_value`: INT,
                  `formula`: STRING,
                  `validator`: STRING,
                  `bit_start`: BIGINT,
                  `bit_length`: BIGINT,
                  `endianness`: INT,
                  `sign`: INT,
                  `emitter_offset`: DOUBLE,
                  `emitter_scale`: DOUBLE,
                  `emitter_min`: DOUBLE,
                  `emitter_max`: DOUBLE,
                  `emitter_unit_conversion_scale`: DOUBLE,
                  `obd_protocol`: INT,
                  `signal_state_encoding`: STRUCT<
                    `state_encoding_enabled`: INT,
                    `mappings`: ARRAY<
                      STRUCT<
                        `input_signal_state`: BIGINT,
                        `output_signal_state`: BIGINT
                      >
                    >,
                    `default_enabled`: INT,
                    `default_value`: BIGINT
                  >
                >
              >,
              `module_address`: BIGINT,
              `module_address_mask`: BIGINT,
              `vehicle_diagnostic_input`: INT,
              `vehicle_diagnostic_bus`: INT
            >
          >,
          `vin`: STRING
        >,
        `passenger_active_command`: ARRAY<
          STRUCT<
            `emitter_config`: ARRAY<
              STRUCT<
                `obd_value`: INT,
                `formula`: STRING,
                `validator`: STRING,
                `bit_start`: BIGINT,
                `bit_length`: BIGINT,
                `endianness`: INT,
                `sign`: INT,
                `emitter_offset`: DOUBLE,
                `emitter_scale`: DOUBLE,
                `emitter_min`: DOUBLE,
                `emitter_max`: DOUBLE,
                `emitter_unit_conversion_scale`: DOUBLE,
                `obd_protocol`: INT,
                `signal_state_encoding`: STRUCT<
                  `state_encoding_enabled`: INT,
                  `mappings`: ARRAY<
                    STRUCT<
                      `input_signal_state`: BIGINT,
                      `output_signal_state`: BIGINT
                    >
                  >,
                  `default_enabled`: INT,
                  `default_value`: BIGINT
                >
              >
            >,
            `module_address`: BIGINT,
            `command`: BINARY,
            `response_address`: BIGINT,
            `read_period_ms`: DECIMAL(20, 0),
            `vehicle_diagnostic_input`: INT,
            `vehicle_diagnostic_bus`: INT
          >
        >,
        `passenger_passive_command`: ARRAY<
          STRUCT<
            `emitter_config`: ARRAY<
              STRUCT<
                `obd_value`: INT,
                `formula`: STRING,
                `validator`: STRING,
                `bit_start`: BIGINT,
                `bit_length`: BIGINT,
                `endianness`: INT,
                `sign`: INT,
                `emitter_offset`: DOUBLE,
                `emitter_scale`: DOUBLE,
                `emitter_min`: DOUBLE,
                `emitter_max`: DOUBLE,
                `emitter_unit_conversion_scale`: DOUBLE,
                `obd_protocol`: INT,
                `signal_state_encoding`: STRUCT<
                  `state_encoding_enabled`: INT,
                  `mappings`: ARRAY<
                    STRUCT<
                      `input_signal_state`: BIGINT,
                      `output_signal_state`: BIGINT
                    >
                  >,
                  `default_enabled`: INT,
                  `default_value`: BIGINT
                >
              >
            >,
            `module_address`: BIGINT,
            `module_address_mask`: BIGINT,
            `vehicle_diagnostic_input`: INT,
            `vehicle_diagnostic_bus`: INT
          >
        >,
        `custom_loggers`: ARRAY<
          STRUCT<
            `obd_value`: INT,
            `object_stat_enum`: INT,
            `min_log_period_ms`: DECIMAL(20, 0),
            `max_log_period_ms`: DECIMAL(20, 0),
            `int_threshold`: BIGINT,
            `disable_flags`: BOOLEAN,
            `transformation_func_enum`: INT
          >
        >,
        `tk_logger`: INT,
        `eco_driver_config`: STRUCT<
          `aggregated_stats_period_ms`: DECIMAL(20, 0),
          `rpm_green_band`: STRUCT<
            `lower_limit_rpm`: INT,
            `upper_limit_rpm`: INT
          >,
          `coasting_max_engine_torque_percent`: BIGINT,
          `accel_min_engine_torque_percent`: BIGINT,
          `anticipation_brake_min_speed_threshold_kmph`: BIGINT,
          `anticipation_brake_deceleration_threshold_kmph_per_second`: BIGINT,
          `anticipation_brake_event_delay_ms`: BIGINT,
          `wear_free_braking_minimum_speed_threshold_kmph`: BIGINT,
          `wear_free_braking_feature_enabled`: INT,
          `accelerator_pedal_position_threshold_percent`: BIGINT,
          `accelerator_pedal_position_threshold_decipercent`: BIGINT,
          `signal_stale_timeout_ms`: BIGINT,
          `accelerator_pedal_position_threshold`: STRUCT<
            `position_a_percent`: BIGINT,
            `position_b_percent`: BIGINT,
            `position_c_percent`: BIGINT,
            `position_d_percent`: BIGINT
          >
        >,
        `disable_prevent_logging_starting_odometer`: BOOLEAN,
        `vg34_enable_usb_to_can_device_for_diagnostics_override`: BOOLEAN,
        `secondary_canbus_bitrate_override`: BIGINT,
        `allow_secondary_canbus_active_mode`: BOOLEAN,
        `spoof_engine_always_on`: BOOLEAN,
        `passenger_passive_buffer_length`: BIGINT,
        `unload_mcp251x_module`: BOOLEAN,
        `enable_ecu_speed_based_engine_state`: BOOLEAN,
        `engine_off_passenger_shutdown_delay_ms`: DECIMAL(20, 0),
        `force_passenger_port_connection`: BOOLEAN,
        `can_bitrate_detection`: STRUCT<
          `possible_bitrates`: ARRAY<BIGINT>,
          `max_listen_ms`: DECIMAL(20, 0),
          `min_data_frames`: BIGINT,
          `min_data_frames_per_error_frame_ratio`: BIGINT,
          `active_mode_min_bitrate_percent`: BIGINT,
          `min_protocol_bit0_frames_per_bit_stuffing_error_frame_ratio`: BIGINT,
          `allow_one_shot_bitrate_detection`: INT,
          `one_shot_frame_send_interval_ms`: BIGINT,
          `one_shot_max_send_frame_count`: BIGINT,
          `primary_bus_bitrate_detection_behavior`: INT,
          `reverse_polarity_for_duckbill_enabled`: INT,
          `enforce_error_frame_ratio_for_duckbill_enabled`: INT
        >,
        `smog_check_tracker_report_period_ms`: DECIMAL(20, 0),
        `ev_charging_config`: STRUCT<
          `ev_min_charging_milli_amp`: BIGINT,
          `ev_charging_timeout_ms`: BIGINT,
          `disable_ev_charging`: BOOLEAN,
          `ev_charging_energy_aggregator_report_period_ms`: BIGINT,
          `enable_engine_off_charging_only`: BOOLEAN,
          `ev_last_speed_received_timeout_ms`: BIGINT,
          `ev_zero_speed_debounce_time_ms`: BIGINT,
          `ev_charging_status_enabled`: INT,
          `ev_charging_status_metadata_enabled`: INT,
          `ev_charging_status_charge_in_progress_enabled`: INT,
          `ev_charging_status_charge_rate_enabled`: INT,
          `ev_charging_status_battery_current_enabled`: INT,
          `wake_on_recent_charging_enabled`: INT,
          `wake_on_recent_charging_threshold_ms`: DECIMAL(20, 0),
          `check_ev_charging_before_reboot_enabled`: INT,
          `check_ev_charging_before_reboot_threshold_ms`: DECIMAL(20, 0)
        >,
        `j1939_request_address`: BIGINT,
        `fuel_level_zero_filter_percent`: DECIMAL(20, 0),
        `state_of_charge_zero_filter_percent`: DECIMAL(20, 0),
        `ev_wake_message_timeout_ms`: BIGINT,
        `enable_j1939_address_claim`: BOOLEAN,
        `disable_j1939_null_addr_claim_fallback`: BOOLEAN,
        `odometer_speed_filter_meters_per_sec`: BIGINT,
        `supported_commands_config`: STRUCT<
          `periodic_request_interval_for_supported_commands_ms`: BIGINT,
          `supported_commands_timeout_ms`: BIGINT,
          `supported_commands_log_interval_ms`: BIGINT,
          `initial_log_delay_ms`: DECIMAL(20, 0),
          `direct_addressing_enabled`: INT
        >,
        `vin_specific_vehicle_info`: STRUCT<
          `fuel_type`: INT,
          `engine_displacement_cm3`: BIGINT,
          `fuel_tank_volume_ml`: DECIMAL(20, 0)
        >,
        `vin`: STRING,
        `disable_maf_on_diesel_vehicles_v2`: BOOLEAN,
        `j1939_auto_detect_config`: STRUCT<
          `j1939_auto_detect_behavior_with_hysteresis`: INT,
          `j1939_pin_auto_detect_result`: BOOLEAN,
          `j1939_auto_detect_hysteresis_threshold`: INT,
          `probe_timeout`: DECIMAL(20, 0),
          `number_of_frames_to_probe_with`: DECIMAL(20, 0),
          `min_matching_pgns`: DECIMAL(20, 0),
          `min_number_of_unique_pgns`: DECIMAL(20, 0),
          `j1939_auto_detect_invalid_log_interval_ms`: DECIMAL(20, 0),
          `j1939_auto_detect_listen_only_mode`: INT
        >,
        `raw_odometer_log_interval_ms`: DECIMAL(20, 0),
        `error_logger_config`: STRUCT<
          `error_config`: ARRAY<
            STRUCT<
              `obd_value`: INT,
              `window_ms`: BIGINT,
              `reporting_ms`: BIGINT,
              `error_rate_percent`: BIGINT
            >
          >,
          `min_check_interval_ms`: BIGINT
        >,
        `emulator_config`: STRUCT<
          `passive_passenger_module`: ARRAY<
            STRUCT<
              `module_address`: BIGINT,
              `messages`: ARRAY<
                STRUCT<
                  `description`: STRING,
                  `data`: BINARY,
                  `period_ms`: BIGINT
                >
              >
            >
          >
        >,
        `j1939_auto_detect_algorithm`: INT,
        `j1939_listen_only`: BOOLEAN,
        `j1939_passive_commands`: ARRAY<
          STRUCT<
            `emitter_configs`: ARRAY<
              STRUCT<
                `obd_value`: INT,
                `formula`: STRING,
                `validator`: STRING,
                `bit_start`: BIGINT,
                `bit_length`: BIGINT,
                `endianness`: INT,
                `sign`: INT,
                `emitter_offset`: DOUBLE,
                `emitter_scale`: DOUBLE,
                `emitter_min`: DOUBLE,
                `emitter_max`: DOUBLE,
                `emitter_unit_conversion_scale`: DOUBLE,
                `obd_protocol`: INT,
                `signal_state_encoding`: STRUCT<
                  `state_encoding_enabled`: INT,
                  `mappings`: ARRAY<
                    STRUCT<
                      `input_signal_state`: BIGINT,
                      `output_signal_state`: BIGINT
                    >
                  >,
                  `default_enabled`: INT,
                  `default_value`: BIGINT
                >
              >
            >,
            `pgn`: BIGINT
          >
        >,
        `speed_max_accel_milliknots_per_sec`: BIGINT,
        `speed_max_decel_milliknots_per_sec`: BIGINT,
        `enable_falko_firebird_compatability`: BOOLEAN,
        `enable_termination_resistor_falko_firebird`: BOOLEAN,
        `fuel_method_priorities_low_to_high`: ARRAY<INT>,
        `disable_filtered_engine_milliknots_logging`: BOOLEAN,
        `tire_condition_logger_report_interval_ms`: BIGINT,
        `tire_condition_eviction_time_ms`: BIGINT,
        `skip_vin_check_digit_wmi_chars`: ARRAY<STRING>,
        `carrier_logger`: INT,
        `bus_configs`: ARRAY<
          STRUCT<
            `obd_probe_type`: INT,
            `vehicle_bus_config`: STRUCT<
              `can_config`: STRUCT<
                `can_type`: INT,
                `high_speed_can_config`: STRUCT<`terminate`: BOOLEAN>,
                `bitrate`: BIGINT,
                `listen_only`: BOOLEAN,
                `can_id_type`: INT,
                `can_polarity`: INT
              >,
              `kline_config`: STRUCT<
                `do_not_run_init`: BOOLEAN,
                `kline_protocol`: INT,
                `intermessage_delay_ms`: DECIMAL(20, 0)
              >,
              `j1850_config`: STRUCT<
                `j1850_mode`: INT,
                `txid`: BIGINT
              >
            >
          >
        >,
        `enable_vin_check_digit_exclusion_char`: BOOLEAN,
        `report_fuel_debug_stat`: BOOLEAN,
        `fuel_method_blacklist`: ARRAY<
          STRUCT<
            `method`: INT,
            `fuel_types`: ARRAY<INT>
          >
        >,
        `passenger_resp_timeout_ms`: BIGINT,
        `j1939_auto_detect_expiration_config`: STRUCT<
          `min_trip_count_before_expire`: DECIMAL(20, 0),
          `min_whitelisted_pgns`: DECIMAL(20, 0)
        >,
        `disable_can_error_frames`: BOOLEAN,
        `can_error_frame_log_interval_ms`: BIGINT,
        `swap_vg_internal_can_with_modi_secondary_can`: BOOLEAN,
        `use_new_loop_for_passenger_vehicles`: BOOLEAN,
        `disable_odometer_lock_expiration_check`: BOOLEAN,
        `min_engine_on_to_expire_odometer_lock_ms`: BIGINT,
        `disable_iso27145`: BOOLEAN,
        `min_fuel_used_ml_infer_tank_size`: BIGINT,
        `disable_tank_level_inference_from_remaining_fuel_ml`: BOOLEAN,
        `falko_vehicle_diagnostic_bus`: INT,
        `j1939_cable_upgrade`: INT,
        `thermoking_cable_upgrade`: INT,
        `vin_specific_allowed_passenger_diagnostic_buses`: ARRAY<INT>,
        `disable_fuel_consumed_lock_expiration_check`: BOOLEAN,
        `min_engine_on_to_expire_fuel_consumed_lock_ms`: BIGINT,
        `prefer_j1939_on_pins_f_g`: BOOLEAN,
        `disable_engine_runtime_lock_expiration_check`: BOOLEAN,
        `min_engine_on_to_expire_engine_runtime_lock_ms`: BIGINT,
        `default_fuel_type`: INT,
        `disable_kline`: BOOLEAN,
        `max_valid_speed_milliknots`: DECIMAL(20, 0),
        `j1939_response_timeout_ms`: BIGINT,
        `persistent_odometer_latest_value_time_to_reset_ms`: DECIMAL(20, 0),
        `disable_persistent_odometer`: BOOLEAN,
        `enable_uds_vin`: BOOLEAN,
        `disable_speed_filter_time_scaling`: BOOLEAN,
        `disable_heavy_duty_vehicle_wait_for_connection_event`: BOOLEAN,
        `j1939_autodetect_type`: INT,
        `j1587_bus_active_config`: STRUCT<
          `min_unique_pids`: BIGINT,
          `listen_timeout_ms`: BIGINT
        >,
        `j1850_enabled_flag`: INT,
        `heavy_duty_j1708_can3_bus_autodetect_config`: STRUCT<
          `j1708_can3_bus_autodetect_method`: INT,
          `can3_autodetect_trip_duration_ms`: BIGINT,
          `j1708_autodetect_trip_duration_ms`: BIGINT,
          `allow_autodetected_bus_start_during_trip`: INT,
          `ecu_engine_source_enabled`: INT,
          `gps_trip_status_valid_duration_ms`: BIGINT,
          `bus_not_present_count_before_cached`: BIGINT,
          `obd_engine_state_bus_message_enabled`: INT
        >,
        `j1708_can3_bus_autodetect_override`: INT,
        `diagnostic_message_config`: STRUCT<
          `disable_diagnostic_message_report`: BOOLEAN,
          `diagnostic_message_report_delay_ms`: BIGINT,
          `enable_last_bytes_read_logging`: BOOLEAN,
          `filtering_config`: STRUCT<
            `stop_broadcast_filtering_enabled`: INT,
            `stop_broadcast_filtering_delay_ms`: BIGINT,
            `stop_broadcast_filtering_window_ms`: BIGINT
          >
        >,
        `disable_prevent_logging_starting_speed`: BOOLEAN,
        `prevent_logging_starting_speed_ms`: BIGINT,
        `raw_logger_config`: ARRAY<
          STRUCT<
            `obd_value`: INT,
            `log_interval_ms`: DECIMAL(20, 0),
            `object_stat_num`: DECIMAL(20, 0)
          >
        >,
        `passenger_can_filter_enabled_flag`: INT,
        `passive_ev_state_of_charge_enabled_flag`: INT,
        `diagnostic_standard_emitter_read_periods`: ARRAY<
          STRUCT<
            `key`: INT,
            `read_period_ms`: DECIMAL(20, 0),
            `disable_read_period`: BOOLEAN,
            `data_identifier`: BIGINT
          >
        >,
        `j1939_commands`: ARRAY<
          STRUCT<
            `emitter_configs`: ARRAY<
              STRUCT<
                `obd_value`: INT,
                `formula`: STRING,
                `validator`: STRING,
                `bit_start`: BIGINT,
                `bit_length`: BIGINT,
                `endianness`: INT,
                `sign`: INT,
                `emitter_offset`: DOUBLE,
                `emitter_scale`: DOUBLE,
                `emitter_min`: DOUBLE,
                `emitter_max`: DOUBLE,
                `emitter_unit_conversion_scale`: DOUBLE,
                `obd_protocol`: INT,
                `signal_state_encoding`: STRUCT<
                  `state_encoding_enabled`: INT,
                  `mappings`: ARRAY<
                    STRUCT<
                      `input_signal_state`: BIGINT,
                      `output_signal_state`: BIGINT
                    >
                  >,
                  `default_enabled`: INT,
                  `default_value`: BIGINT
                >
              >
            >,
            `pgn`: BIGINT,
            `request_interval_ms`: DECIMAL(20, 0)
          >
        >,
        `diagnostic_lock_configs`: ARRAY<
          STRUCT<
            `obd_values_high_to_low_priority`: ARRAY<INT>,
            `disable`: BOOLEAN,
            `disable_expire`: BOOLEAN,
            `delay_set_ms`: DECIMAL(20, 0),
            `expire_ms`: DECIMAL(20, 0),
            `preferred_tx_id`: DECIMAL(20, 0)
          >
        >,
        `filtered_engine_milliknots_logging_enabled_flag`: INT,
        `vin_blocklist`: ARRAY<STRING>,
        `j1939_allow_rts_cts_virtual_connection`: INT,
        `ecu_speed_bus_rate_limiting_enabled`: INT,
        `ev_active_requests_while_stationary`: INT,
        `ev_active_request_rate_while_stationary_limit_ms`: BIGINT,
        `ecu_speed_bus_min_publish_interval_ms`: INT,
        `j1939_dm2_logging_enabled_flag`: INT,
        `obd_lock_config`: STRUCT<
          `obd_lock_mode`: INT,
          `default_delay_ms`: DECIMAL(20, 0),
          `default_expire_after_ms`: DECIMAL(20, 0),
          `clear_lock_check_interval_ms`: DECIMAL(20, 0),
          `obd_value_specific_lock_configs`: ARRAY<
            STRUCT<
              `obd_value`: INT,
              `delay_ms`: DECIMAL(20, 0),
              `expire_after_ms`: DECIMAL(20, 0),
              `preferred_source`: STRUCT<
                `vehicle_diagnostic_bus`: INT,
                `msg_id`: DECIMAL(20, 0),
                `tx_id`: DECIMAL(20, 0),
                `use_vehicle_diagnostic_bus`: INT,
                `use_msg_id`: INT,
                `use_tx_id`: INT
              >,
              `must_match_preferred_source`: BOOLEAN,
              `allow_value_processing_when_engine_off`: BOOLEAN
            >
          >
        >,
        `j1939_component_id_logging`: INT,
        `vehicle_diagnostic_bus_bitrate_overrides`: ARRAY<
          STRUCT<
            `vehicle_diagnostic_bus`: INT,
            `bitrate_override`: BIGINT
          >
        >,
        `seatbelt_alert_limit`: STRUCT<
          `enabled`: INT,
          `max_alerts_per_trip`: BIGINT,
          `alerts_allowed_duration_since_trip_start_ms`: BIGINT,
          `never_alert`: BOOLEAN
        >,
        `mcp251x_hardware_filters`: ARRAY<
          STRUCT<
            `can_bus_type`: BIGINT,
            `rxb0`: STRUCT<
              `enabled`: INT,
              `rxf0`: BIGINT,
              `rxf1`: BIGINT,
              `rxm0`: BIGINT
            >,
            `rxb1`: STRUCT<
              `enabled`: INT,
              `rxf2`: BIGINT,
              `rxf3`: BIGINT,
              `rxf4`: BIGINT,
              `rxf5`: BIGINT,
              `rxm1`: BIGINT
            >
          >
        >,
        `feature_settings`: ARRAY<
          STRUCT<
            `object_stat_enum`: INT,
            `method_id_high_to_low_priority`: ARRAY<INT>,
            `object_stat_state_config`: STRUCT<
              `min_period_ms`: DECIMAL(20, 0),
              `max_period_ms`: DECIMAL(20, 0),
              `int_threshold`: DECIMAL(20, 0),
              `state_flag_status`: INT,
              `use_inclusive_int_threshold`: BOOLEAN
            >,
            `enable_reporting_object_stat`: INT,
            `enable_reporting_all_methods`: INT
          >
        >,
        `report_idle_without_movement_enabled`: INT,
        `j1850_enabled`: INT,
        `seed_runtime_before_engine_state_check_enabled`: INT,
        `scantool_coexistence_config`: STRUCT<
          `scantool_coexistence_method`: INT,
          `anomaly_event_report_interval_ms`: BIGINT,
          `anomaly_event_reporting_enabled`: INT,
          `object_stat_report_interval_ms`: BIGINT,
          `object_stat_reporting_enabled`: INT,
          `diagnostic_loop_inactive_duration_ms`: BIGINT
        >,
        `diagnostic_requests_disabled_after_movement_stop_ms`: DECIMAL(20, 0),
        `signal_cache_config`: STRUCT<
          `snapshot_upload_period_ms`: DECIMAL(20, 0),
          `enabled`: INT,
          `logging_enabled`: INT
        >,
        `obd_max_speed_alert_enabled`: INT,
        `enable_stop_uhdl_diagnostics_while_engine_off`: INT,
        `start_uvl_on_panic_button_press`: INT,
        `enable_logging_j1939_claimed_address`: INT,
        `command_scheduler_config`: STRUCT<
          `expire_commands_enabled`: INT,
          `logging_enabled`: INT,
          `log_interval_ms`: BIGINT,
          `max_no_response_to_expire_command_count_floor`: BIGINT,
          `max_no_response_to_expire_command_count_ceiling`: BIGINT,
          `enqueue_jitter_enabled`: INT,
          `enqueue_jitter_ms`: BIGINT,
          `minimum_jitter_period_multiple`: BIGINT,
          `derate_commands_enabled`: INT,
          `max_no_response_to_derate_command_count_floor`: BIGINT,
          `max_no_response_to_derate_command_count_ceiling`: BIGINT,
          `derated_command_interval_ms`: BIGINT,
          `window_interval_ms`: BIGINT,
          `command_derate_exclusions`: ARRAY<
            STRUCT<
              `request_id`: BIGINT,
              `use_request_id`: BOOLEAN,
              `data_identifier`: DECIMAL(20, 0),
              `use_data_identifier`: BOOLEAN,
              `vehicle_diagnostic_bus`: INT,
              `use_vehicle_diagnostic_bus`: BOOLEAN
            >
          >
        >,
        `signal_multiplexer`: STRUCT<
          `max_multiplexed_signals`: BIGINT,
          `signal`: ARRAY<
            STRUCT<
              `input_signal_value`: INT,
              `output_signal_value`: ARRAY<INT>
            >
          >
        >,
        `setup_mcp251x_delay_ms`: BIGINT,
        `engine_off_from_active_engine_frame_timeout_ms`: BIGINT,
        `engine_off_from_ecu_speed_timeout_ms`: BIGINT,
        `enable_wake_on_voltage_jump`: INT,
        `enable_wake_on_movement`: INT,
        `bus_manager_buffered_message_length`: BIGINT,
        `max_fuel_consumed_delta_milli_l`: BIGINT,
        `can_protocol_autodetect_config`: STRUCT<
          `can_j1939_protocol_detection_method`: INT,
          `enable_requested_j1939_address_claim_protocol_detection`: INT,
          `broadcast_j1939_loop_detection_timeout_ms`: BIGINT,
          `can_passenger_detection_method`: INT,
          `passenger_probe_send_attempts`: BIGINT,
          `reverse_passenger_protocol_detect_order`: INT,
          `allow_preferred_protocol_retry`: INT,
          `j1939_detection_on_all_buses_enabled`: INT,
          `broadcast_pgn_allowlist_enabled`: INT
        >,
        `infer_eld_data_lines_opened`: INT,
        `use_vehicle_and_scheduler_v2_flag`: INT,
        `j1939_address_claim_enabled`: INT,
        `enable_wake_on_can`: INT,
        `iso27145_dtc_emitters_enabled`: INT,
        `harsh_history_store`: STRUCT<
          `publish_accel_pedal_enabled`: INT,
          `publish_brake_pedal_enabled`: INT,
          `publish_ecu_speed_enabled`: INT,
          `publish_cruise_control_switch_enabled`: INT,
          `publish_forward_vehicle_speed_enabled`: INT,
          `publish_forward_vehicle_distance_enabled`: INT,
          `publish_ecu_distance_alert_signal_enabled`: INT,
          `publish_ecu_forward_collision_warning_enabled`: INT,
          `publish_ecu_external_acceleration_demand_enabled`: INT
        >,
        `odometer_start_delay_ms`: DECIMAL(20, 0),
        `odometer_start_delay_ms_valid`: BOOLEAN,
        `odometer_log_max_period_ms`: DECIMAL(20, 0),
        `odometer_log_max_period_ms_valid`: BOOLEAN,
        `odometer_report_threshold_meters`: DECIMAL(20, 0),
        `odometer_report_threshold_meters_valid`: BOOLEAN,
        `odometer_always_report_on_disconnect_enabled`: INT,
        `engine_seconds_log_threshold_seconds`: DECIMAL(20, 0),
        `engine_seconds_log_threshold_seconds_valid`: BOOLEAN,
        `engine_seconds_log_max_period_ms`: DECIMAL(20, 0),
        `engine_seconds_log_max_period_ms_valid`: BOOLEAN,
        `engine_seconds_always_report_on_disconnect_enabled`: INT,
        `enable_obd_trace_test_mode`: BOOLEAN,
        `turn_signal_logger`: STRUCT<
          `logger_enabled`: INT,
          `turn_signal_source`: INT
        >,
        `test_logger_enabled`: INT,
        `j1939_address_claim_bus`: STRUCT<
          `address_range`: STRUCT<
            `use_address_range_override_valid`: BOOLEAN,
            `address_start`: BIGINT,
            `address_end`: BIGINT
          >,
          `name_field`: STRUCT<
            `name_override_valid`: BOOLEAN,
            `arbitrary_address_capable`: BOOLEAN,
            `industry_group`: BIGINT,
            `vehicle_system_instance`: BIGINT,
            `vehicle_system`: BIGINT,
            `function`: BIGINT,
            `function_instance`: BIGINT,
            `ecu_instance`: BIGINT,
            `manufacturer_code`: BIGINT,
            `identity_number`: BIGINT
          >,
          `logging_j1939_claimed_address_enabled`: INT,
          `j1939_null_addr_claim_fallback_enabled`: INT
        >,
        `filter_cache_engine_runtime`: STRUCT<
          `filter_cache_enabled`: INT,
          `delta_filter`: STRUCT<
            `filter_enabled`: INT,
            `delta_range_override_valid`: BOOLEAN,
            `delta_min_value`: INT,
            `delta_max_value`: INT
          >,
          `cache`: STRUCT<
            `cache_file_enabled`: INT,
            `expiration_override_valid`: BOOLEAN,
            `expire_after_ms`: DECIMAL(20, 0)
          >,
          `metric_aggregator`: STRUCT<
            `enabled`: INT,
            `report_interval_ms`: BIGINT
          >,
          `filter_cache_v2_enabled`: INT
        >,
        `ecu_fuel_level_logger_config`: STRUCT<
          `enabled`: INT,
          `log_interval_ms`: BIGINT,
          `allow_millipercent_obd_value`: INT
        >,
        `address_claimed_name_logger_config`: STRUCT<
          `enable_address_claimed_names_logging`: INT,
          `address_claimed_names_log_interval_ms`: DECIMAL(20, 0)
        >,
        `mcp251x_qup_driver_enabled`: INT,
        `tell_tale_status_config`: STRUCT<
          `tts_enabled`: INT,
          `stabilization_time_ms`: BIGINT,
          `report_interval_ms`: BIGINT,
          `report_if_not_available_enabled`: INT,
          `fms_standard_tts_indicators_enabled`: INT,
          `indicator_config`: ARRAY<
            STRUCT<
              `indicator`: INT,
              `group`: BIGINT,
              `index`: BIGINT,
              `indicator_level_config`: STRUCT<
                `default_on_level`: INT,
                `level_by_conditions`: ARRAY<
                  STRUCT<
                    `condition`: INT,
                    `level`: INT
                  >
                >
              >
            >
          >
        >,
        `require_movement_for_engine_idle_enabled`: INT,
        `engine_rpm_logger`: STRUCT<
          `logger_enabled`: INT,
          `logging_interval_ms`: BIGINT
        >,
        `obd_engine_state_bus_publish_enabled`: INT,
        `obd_engine_state_bus_publish_period_ms`: BIGINT,
        `j1587_rpm_based_engine_state_enabled`: INT,
        `j1939_bus_stats`: STRUCT<
          `collect_and_log_enabled`: INT,
          `max_number_of_multi_frame_infos_to_log`: BIGINT,
          `log_period_ms`: DECIMAL(20, 0)
        >,
        `optional_diagnostics_config`: STRUCT<
          `enabled`: INT,
          `ev_config`: STRUCT<
            `enabled`: INT,
            `threshold_mv`: DECIMAL(20, 0),
            `threshold_mv_is_valid`: BOOLEAN
          >,
          `obd_engine_state_bus_message_enabled`: INT
        >,
        `engine_state_movement_caching_enabled`: INT,
        `engine_state_movement_expiration_ms`: DECIMAL(20, 0),
        `async_bus_detection_config`: STRUCT<
          `uhdl_async_bus_detection_enabled`: INT,
          `expiration_seconds`: DECIMAL(20, 0)
        >,
        `deprecated_dm1_fault_ingestion_enabled`: INT,
        `propulsion_system_based_engine_state_enabled`: INT,
        `engine_runtime_frozen_filter_enabled`: INT,
        `can_recorder`: STRUCT<
          `enabled`: INT,
          `recording_from_config_enabled`: INT,
          `bus_publishing_enabled`: INT,
          `file_writing_enabled`: INT,
          `low_res_timestamping_enabled`: INT,
          `low_res_timestamping_resolution_ms`: BIGINT
        >,
        `replace_obd_dump_with_can_recorder_enabled`: INT,
        `gear_state_logger`: STRUCT<`logger_enabled`: INT>,
        `reversing_event_logger`: STRUCT<
          `logger_enabled`: INT,
          `start_event_min_speed_kmph`: DECIMAL(20, 0),
          `end_event_min_forward_speed_kmph`: DECIMAL(20, 0),
          `end_event_min_forward_distance_meters`: DECIMAL(20, 0),
          `end_event_min_time_not_in_reverse_secs`: DECIMAL(20, 0),
          `forward_movement_min_speed_kmph`: DECIMAL(20, 0),
          `forward_movement_min_distance_meters`: DECIMAL(20, 0),
          `max_valid_speed_kmph`: DECIMAL(20, 0)
        >,
        `obd_engine_state_bus_publish_v2_enabled`: INT,
        `tire_condition_manufacturer_config`: STRUCT<
          `force_bendix_enabled`: INT,
          `force_allowed_src_id_enabled`: INT,
          `allowed_src_id`: DECIMAL(20, 0),
          `mark_no_address_claim_as_universal_j1939_enabled`: INT,
          `force_ecu_to_manufacturer`: ARRAY<
            STRUCT<
              `src_id`: DECIMAL(20, 0),
              `manufacturer_code`: BIGINT
            >
          >,
          `custom_manufacturers`: ARRAY<
            STRUCT<
              `manufacturer_code`: BIGINT,
              `manufacturer`: BIGINT
            >
          >
        >,
        `isuzu_secondary_bus_enabled`: INT,
        `harsh_event_trigger`: STRUCT<
          `enabled`: INT,
          `debounce_ms`: BIGINT
        >,
        `bus_connection_states_logging_enabled`: INT,
        `seatbelt_reporting`: STRUCT<
          `additional_osdseatbeltdriver_stats_enabled`: INT,
          `minimum_speed_mph`: BIGINT,
          `log_max_period_ms`: BIGINT,
          `seatbelt_bus_publish_enabled`: INT
        >,
        `j1699_test_detection_config`: STRUCT<
          `j1699_test_detection_enabled`: INT,
          `general_loop_inactive_duration_ms`: BIGINT,
          `general_func_req_service01_pid00_timeout_ms`: BIGINT,
          `general_func_req_service01_pid0d_short_circuit_ms`: BIGINT,
          `general_non_zero_ecu_speed_short_circuit_ms`: BIGINT,
          `j1699_detection_parameters`: ARRAY<
            STRUCT<
              `obd_value`: INT,
              `depends_on_func_req_service01_pid0d`: BOOLEAN,
              `func_req_service01_pid0d_short_circuit_ms`: BIGINT,
              `depends_on_non_zero_ecu_speed`: BOOLEAN,
              `non_zero_ecu_speed_short_circuit_ms`: BIGINT,
              `detection_reason`: INT
            >
          >,
          `region_check_enabled`: INT,
          `bounding_box_min_latitude_nd`: BIGINT,
          `bounding_box_max_latitude_nd`: BIGINT,
          `bounding_box_min_longitude_nd`: BIGINT,
          `bounding_box_max_longitude_nd`: BIGINT
        >,
        `advanced_braking_assist_j1939_loggers_enabled`: INT,
        `d1_logger_config`: STRUCT<
          `j1939_lamp_metrics_enabled`: INT,
          `ignore_spn0_fmi0_dtcs_enabled`: INT
        >,
        `engine_activity`: STRUCT<
          `invoke_callback_and_log_interval_ms`: DECIMAL(20, 0),
          `signal_stale_timeout_ms`: DECIMAL(20, 0),
          `test_nonzero_obd_value`: INT,
          `test_monotonically_increasing_obd_value`: INT,
          `test_nonzero_and_changing_obd_value`: INT,
          `test_nonzero_with_debounce_obd_value`: INT,
          `use_engine_activity_internal_for_engine_state_enabled`: INT,
          `engine_state_log_only_enabled`: INT,
          `log_engine_activity_method_min_period_ms`: DECIMAL(20, 0),
          `log_engine_activity_method_enabled`: INT
        >,
        `cansock`: STRUCT<
          `can_error_metrics_enabled`: INT,
          `can_error_metrics_interval_ms`: BIGINT,
          `protocol_error_mask`: BIGINT,
          `protocol_error_mask_is_valid`: BOOLEAN,
          `controller_error_mask`: BIGINT,
          `controller_error_mask_is_valid`: BOOLEAN
        >,
        `wake_on_movement_when_wake_on_can_enabled`: INT,
        `can_auto_responses`: ARRAY<
          STRUCT<
            `trigger_id`: BIGINT,
            `trigger_payload`: BINARY,
            `trigger_payload_mask`: BINARY,
            `response_id`: BIGINT,
            `response_payload`: BINARY,
            `response_len`: BIGINT,
            `bus_id`: INT
          >
        >,
        `message_bus_publishers`: ARRAY<
          STRUCT<
            `message_type`: INT,
            `obd_value`: INT,
            `min_publish_interval_ms`: DECIMAL(20, 0),
            `max_publish_interval_ms`: DECIMAL(20, 0),
            `int_threshold`: DECIMAL(20, 0),
            `rate_limiting_mode`: INT
          >
        >,
        `allow_j1939_on_cable_dj1939vm_pin6_and_pin14`: INT,
        `telemetry_publishers`: ARRAY<
          STRUCT<
            `subtopic`: STRING,
            `obd_value`: INT,
            `value_type`: INT,
            `publish_on_change_only`: BOOLEAN,
            `int_change_threshold`: DECIMAL(20, 0),
            `min_publish_interval_ms`: DECIMAL(20, 0),
            `max_publish_interval_ms`: DECIMAL(20, 0)
          >
        >,
        `can_interface_manager_enabled`: INT,
        `freeze_frame_config`: STRUCT<`logging_enabled`: INT>,
        `autodetect_towing_enabled`: INT,
        `duckbill_config`: STRUCT<
          `uvl_enabled`: INT,
          `uvl_require_detection_enabled`: INT,
          `uhdl_passenger_bus_enabled`: INT,
          `autodetection_enabled`: INT
        >,
        `electronic_brake_performance_monitoring_system`: STRUCT<
          `ebpms`: STRUCT<
            `ebpms_enable`: INT,
            `min_brake_event_duration_ms`: BIGINT,
            `max_brake_event_duration_ms`: BIGINT,
            `min_entry_speed_kph`: BIGINT,
            `min_exit_speed_kph`: BIGINT,
            `min_entry_demand_pressure_kpa`: BIGINT,
            `min_exit_demand_pressure_kpa`: BIGINT,
            `event_sample_rate_ms`: BIGINT
          >
        >,
        `eld_odometer_logger`: STRUCT<`logger_enabled`: INT>,
        `segmented_events`: STRUCT<
          `enabled`: INT,
          `event_conditions`: ARRAY<
            STRUCT<
              `object_stat_enum`: INT,
              `condition_groups`: ARRAY<
                STRUCT<
                  `conditions`: ARRAY<
                    STRUCT<
                      `obd_value`: INT,
                      `threshold_value`: BIGINT,
                      `operator`: INT,
                      `min_duration_ms`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `use_audio_alert_policy_manager_for_seatbelt_alerts_enabled`: INT,
        `eld_speed_logger`: STRUCT<
          `logger_enabled`: INT,
          `speed_filter_enabled`: INT,
          `startup_filter_enabled`: INT,
          `object_stat_logging_enabled`: INT
        >,
        `manual_synthetic_engine_runtime`: STRUCT<
          `enabled`: INT,
          `updated_at_ms`: DECIMAL(20, 0),
          `offset_seconds`: DECIMAL(20, 0),
          `zero_updated_at_ms_processing_enabled`: INT
        >,
        `tire_condition_custom_emitters`: ARRAY<
          STRUCT<
            `pgn`: BIGINT,
            `axle_config`: STRUCT<
              `obd_value`: INT,
              `formula`: STRING,
              `validator`: STRING,
              `bit_start`: BIGINT,
              `bit_length`: BIGINT,
              `endianness`: INT,
              `sign`: INT,
              `emitter_offset`: DOUBLE,
              `emitter_scale`: DOUBLE,
              `emitter_min`: DOUBLE,
              `emitter_max`: DOUBLE,
              `emitter_unit_conversion_scale`: DOUBLE,
              `obd_protocol`: INT,
              `signal_state_encoding`: STRUCT<
                `state_encoding_enabled`: INT,
                `mappings`: ARRAY<
                  STRUCT<
                    `input_signal_state`: BIGINT,
                    `output_signal_state`: BIGINT
                  >
                >,
                `default_enabled`: INT,
                `default_value`: BIGINT
              >
            >,
            `tire_config`: STRUCT<
              `obd_value`: INT,
              `formula`: STRING,
              `validator`: STRING,
              `bit_start`: BIGINT,
              `bit_length`: BIGINT,
              `endianness`: INT,
              `sign`: INT,
              `emitter_offset`: DOUBLE,
              `emitter_scale`: DOUBLE,
              `emitter_min`: DOUBLE,
              `emitter_max`: DOUBLE,
              `emitter_unit_conversion_scale`: DOUBLE,
              `obd_protocol`: INT,
              `signal_state_encoding`: STRUCT<
                `state_encoding_enabled`: INT,
                `mappings`: ARRAY<
                  STRUCT<
                    `input_signal_state`: BIGINT,
                    `output_signal_state`: BIGINT
                  >
                >,
                `default_enabled`: INT,
                `default_value`: BIGINT
              >
            >,
            `field_configs`: ARRAY<
              STRUCT<
                `obd_value`: INT,
                `formula`: STRING,
                `validator`: STRING,
                `bit_start`: BIGINT,
                `bit_length`: BIGINT,
                `endianness`: INT,
                `sign`: INT,
                `emitter_offset`: DOUBLE,
                `emitter_scale`: DOUBLE,
                `emitter_min`: DOUBLE,
                `emitter_max`: DOUBLE,
                `emitter_unit_conversion_scale`: DOUBLE,
                `obd_protocol`: INT,
                `signal_state_encoding`: STRUCT<
                  `state_encoding_enabled`: INT,
                  `mappings`: ARRAY<
                    STRUCT<
                      `input_signal_state`: BIGINT,
                      `output_signal_state`: BIGINT
                    >
                  >,
                  `default_enabled`: INT,
                  `default_value`: BIGINT
                >
              >
            >,
            `emit_raw`: BOOLEAN
          >
        >
      >,
      `auto_sleep`: STRUCT<
        `sleep_delay_secs`: INT,
        `magnitude_threshold`: FLOAT,
        `sleep_duration_secs`: INT,
        `sleep_wake_duration_secs`: INT,
        `wake_on_battery`: BOOLEAN,
        `wake_on_power`: BOOLEAN,
        `sleep_override`: STRUCT<
          `override_timeout_secs`: INT,
          `sleep_duration_secs`: INT,
          `sleep_wake_duration_secs`: INT,
          `sleep_behavior`: INT,
          `use_persistent_override_timeout`: BOOLEAN,
          `persistent_override_timeout_secs`: BIGINT
        >,
        `wake_on_movement`: BOOLEAN,
        `wake_on_wifi_clients`: BOOLEAN,
        `sleep_behavior`: INT,
        `wake_while_no_gps`: BOOLEAN,
        `max_wake_secs`: INT,
        `accel_wake_config`: STRUCT<
          `high_pass_filter`: INT,
          `threshold`: INT,
          `duration`: INT
        >,
        `accel_enable_delay_secs`: BIGINT,
        `scheduled_wake_config`: STRUCT<
          `wake_at_times`: ARRAY<STRING>,
          `timezone`: STRING
        >,
        `enabled`: BOOLEAN,
        `wake_on_voltage_jump`: BOOLEAN,
        `voltage_jump_sleep_delay_secs`: BIGINT,
        `min_accel_interrupt_count`: BIGINT,
        `report_upload_sleep_delay_secs`: BIGINT,
        `obd_manager_active_in_moderate_power`: BOOLEAN,
        `disable_location_manager_in_moderate_power`: BOOLEAN,
        `disable_wifi_in_moderate_power`: BOOLEAN,
        `enable_widget_mgr_in_moderate_power`: BOOLEAN,
        `wake_on_panic_button`: BOOLEAN,
        `panic_button_wake_duration_ms`: BIGINT,
        `keep_wifi_bt_vreg_enabled_in_low_power`: BOOLEAN,
        `disable_force_ship_mode_power_config`: BOOLEAN,
        `required_work_sleep_delays`: ARRAY<
          STRUCT<
            `work_type`: INT,
            `max_delay_ms`: BIGINT
          >
        >,
        `hub_report_delay_after_required_work_ms`: BIGINT,
        `extend_full_power_reasons`: ARRAY<INT>,
        `reason_sleep_delay_ms_map`: ARRAY<
          STRUCT<
            `reason`: INT,
            `sleep_delay_ms`: DECIMAL(20, 0)
          >
        >,
        `enable_cellular_manager_in_low_power`: INT,
        `suspend_in_low_power`: INT,
        `enable_required_work_v2`: INT,
        `enable_usb_hub_power_off_delay`: INT,
        `usb_hub_power_off_delay_secs`: BIGINT,
        `keep_mcu_in_full_power_when_usb_hub_powered`: INT,
        `wake_on_movement_min_cable_millivolts`: BIGINT,
        `wake_on_can`: INT,
        `log_can_wake_event`: INT,
        `mdm9607_skip_full_power_between_moderate_to_low_power_transition_enabled`: INT,
        `boot_with_power_saving`: STRUCT<
          `enabled`: INT,
          `power_saving_reboot_reasons`: ARRAY<INT>,
          `normal_power_duration_secs`: BIGINT
        >,
        `wake_reason_stat_enabled`: INT,
        `wake_on_equipment_activity_enabled`: INT,
        `wake_on_voltage_jump_delay_enabled`: INT,
        `wake_on_voltage_jump_delay_ms`: BIGINT,
        `wake_on_movement_when_wake_on_can_enabled`: INT,
        `wake_on_aux_input_config`: STRUCT<
          `enabled`: INT,
          `sleep_delay_ms`: DECIMAL(20, 0),
          `aux_input_1_wake_config`: STRUCT<`wake_enabled`: INT>,
          `aux_input_2_wake_config`: STRUCT<`wake_enabled`: INT>,
          `aux_input_3_wake_config`: STRUCT<`wake_enabled`: INT>,
          `aux_input_4_wake_config`: STRUCT<`wake_enabled`: INT>,
          `aux_input_5_wake_config`: STRUCT<`wake_enabled`: INT>
        >,
        `brief_moderate_with_satellite_enabled`: INT,
        `brief_moderate_sleep_duration_secs`: BIGINT,
        `brief_moderate_sleep_wake_duration_secs`: BIGINT,
        `autodetect_towing_enabled`: INT,
        `wake_on_ble_sos_with_satellite_enabled`: INT,
        `wake_on_ble_sos_valid_duration_secs`: BIGINT,
        `wake_on_ble_sos_sleep_delay_duration_secs`: BIGINT
      >,
      `ssh_config`: STRUCT<
        `enable`: BOOLEAN,
        `entry`: ARRAY<
          STRUCT<`key`: STRING>
        >
      >,
      `ble_advertisement_interval_max_ms`: INT,
      `usb_console_enabled`: BOOLEAN,
      `obd_probe_type`: INT,
      `org_id`: BIGINT,
      `driver_id`: BIGINT,
      `obd_passenger_wait_for_movement`: BOOLEAN,
      `gps_min_update_ms`: INT,
      `movement_magnitude_threshold`: FLOAT,
      `gps_config`: STRUCT<
        `min_update_ms`: INT,
        `trigger_mph`: INT,
        `trigger_on_stop`: BOOLEAN,
        `trigger_heading_degrees`: INT,
        `trigger_distance_degree_b`: BIGINT,
        `dop_threshold`: FLOAT,
        `gps_spoof_entries`: ARRAY<
          STRUCT<
            `delay_ms`: BIGINT,
            `latitude`: BIGINT,
            `longitude`: BIGINT
          >
        >,
        `stationary_dop_threshold`: FLOAT,
        `stationary_snap_meters`: BIGINT,
        `trigger_on_accel_movement_change`: BOOLEAN,
        `dump_startup_data`: BOOLEAN,
        `trigger_distance_meters`: BIGINT,
        `update_movement_threshold_mph`: BIGINT,
        `extend_movement_window_ms`: BIGINT,
        `log_no_fix_anomaly_at_shutdown`: BOOLEAN,
        `disable_gps_as_time_source`: BOOLEAN,
        `expire_old_fix_ms`: BIGINT,
        `force_log_ms`: BIGINT,
        `beacon_to_gps_delay_ms`: BIGINT,
        `mdm9607`: STRUCT<
          `min_reset_delay_ms`: BIGINT,
          `gps_extended_data_interval_ms`: DECIMAL(20, 0),
          `enable_sbas_waas`: BOOLEAN,
          `enable_sbas_egnos`: BOOLEAN,
          `enable_sbas_msas`: BOOLEAN,
          `enable_sbas_gagan`: BOOLEAN,
          `galileo_config`: INT,
          `beidou_config`: INT,
          `qzss_config`: INT,
          `enable_reboot_on_nv_change`: BOOLEAN,
          `disable_qmi_intermediate_fixes`: BOOLEAN,
          `disable_qmi_high_accuracy`: BOOLEAN,
          `orbit_data_urls`: ARRAY<STRING>,
          `passive_antenna_enabled`: BOOLEAN,
          `sbas_waas`: INT,
          `sbas_egnos`: INT,
          `sbas_msas`: INT,
          `sbas_gagan`: INT,
          `galileo_outside_us`: INT,
          `galileo_worldwide`: INT,
          `beidou_outside_us`: INT,
          `beidou_worldwide`: INT,
          `qzss_outside_us`: INT,
          `qzss_worldwide`: INT,
          `reboot_on_nv_change`: INT,
          `use_dead_reckoning_engine`: INT,
          `log_dead_reckoning_debug_events`: INT,
          `dead_reckoning_debug_force_log_seconds`: BIGINT,
          `dead_reckoning_verbose_local_log`: INT,
          `log_gps_reception_info_stat`: INT,
          `time_injection`: INT,
          `orbit_injection`: INT,
          `jamming_detection`: STRUCT<
            `enabled`: INT,
            `verbose_logging`: INT,
            `send_raw_metrics`: INT,
            `persist_reference_noise_level`: INT,
            `reference_noise_level_max_gps_accuracy_millimeters`: BIGINT,
            `min_jamming_duration_seconds`: BIGINT,
            `max_reporting_interval_seconds`: BIGINT,
            `spoof_entries`: ARRAY<
              STRUCT<
                `spoof_status_duration_seconds`: BIGINT,
                `spoof_status`: INT
              >
            >,
            `max_not_jammed_gps_accuracy_millimeters`: BIGINT,
            `jammer_metric_threshold_gps_millidb`: BIGINT,
            `jammer_metric_threshold_glo_millidb`: BIGINT,
            `jammer_metric_threshold_gal_millidb`: BIGINT,
            `jammer_metric_threshold_bds_millidb`: BIGINT,
            `receiver_metrics_log_always_enabled`: INT,
            `receiver_metrics_log_on_gps_jammed_enabled`: INT,
            `receiver_metrics_log_on_hub_server_disconnected_enabled`: INT,
            `receiver_metrics_log_on_position_incomplete_enabled`: INT,
            `receiver_metrics_log_on_tampered_enabled`: INT,
            `max_acceptable_pga_gain_while_jammed`: INT,
            `max_acceptable_pga_gain_while_jammed_valid`: BOOLEAN,
            `min_number_of_constellations_exceeding_thresholds`: BIGINT,
            `reference_noise_level_max_adc_amplitude`: BIGINT,
            `reference_noise_level_filter_window_length`: BIGINT,
            `reference_noise_level_filter_max_window_duration_seconds`: BIGINT,
            `reference_noise_level_min_acceptable_pga_gain_enabled`: INT,
            `reference_noise_level_min_acceptable_pga_gain`: INT
          >,
          `no_gps_data_reboot_ms`: DECIMAL(20, 0),
          `good_snr_low_fix`: STRUCT<
            `reboot_when_good_snr_but_no_fix_enabled`: INT,
            `min_good_snr_count_per_min`: BIGINT,
            `max_gps_position_report_count_per_min`: BIGINT,
            `min_snr`: FLOAT
          >,
          `qmi_location_client_failure`: STRUCT<
            `reboot_on_failure_to_start_gps_location_client_enabled`: INT,
            `max_successive_start_gps_location_client_panic_threshold`: BIGINT
          >
        >,
        `save_persistent_gps_log_fix`: BOOLEAN,
        `stationary_min_log_after_first_fix_ms`: DECIMAL(20, 0),
        `gps_persistent_override`: STRUCT<
          `latitude`: FLOAT,
          `longitude`: FLOAT
        >,
        `min_heading_trigger_mph`: FLOAT,
        `location_stop_trigger`: STRUCT<
          `enabled`: BOOLEAN,
          `recent_within_ms`: DECIMAL(20, 0),
          `recent_max_meters`: FLOAT,
          `older_within_ms`: DECIMAL(20, 0),
          `older_min_meters`: FLOAT
        >,
        `stationary_min_log_after_first_fix_mph`: FLOAT,
        `regain_fix_stationary_mph`: FLOAT,
        `trigger_on_gps_start_and_stop`: BOOLEAN,
        `trigger_on_accel_movement_start_and_stop`: BOOLEAN,
        `start_trip_speed_threshold_mph`: DOUBLE,
        `end_trip_speed_threshold_mph`: DOUBLE,
        `trip_dwell_threshold_ms`: DECIMAL(20, 0),
        `log_gps_health`: INT,
        `gps_health_interval_secs`: BIGINT,
        `publish_trip_state_interval_ms`: BIGINT,
        `manual_synthetic_odometer_meters`: STRUCT<
          `enabled`: INT,
          `offset_meters`: DECIMAL(20, 0),
          `updated_at_ms`: DECIMAL(20, 0),
          `zero_updated_at_ms_processing_enabled`: INT
        >
      >,
      `cellular_config`: STRUCT<
        `lcp_echo_interval`: BIGINT,
        `lcp_echo_failure`: BIGINT,
        `apn`: STRING,
        `debug_log_interval_ms`: BIGINT,
        `end_data_call_for_periodic_debug_log`: BOOLEAN,
        `debug_log_at_startup`: BOOLEAN,
        `end_data_call_for_startup_debug_log`: BOOLEAN,
        `startup_debug_log_delay_ms`: BIGINT,
        `sim_slot`: INT,
        `secondary_carrier_params`: ARRAY<
          STRUCT<
            `carrier_name`: STRING,
            `apn`: STRING,
            `override_default`: BOOLEAN
          >
        >,
        `ue_usage_setting`: STRUCT<
          `enabled`: INT,
          `setting`: INT
        >,
        `sim_switch_try_configured_sim_interval_ms`: DECIMAL(20, 0),
        `sim_slot_0_failover_after_ms`: DECIMAL(20, 0),
        `sim_slot_1_failover_after_ms`: DECIMAL(20, 0),
        `qcom_internal_err_recovery`: STRUCT<
          `enabled`: INT,
          `modem_reset_secs`: BIGINT,
          `reboot_secs`: BIGINT
        >,
        `no_hub_connection_recovery`: STRUCT<
          `call_restart_enabled`: INT,
          `call_restart_secs`: BIGINT,
          `modem_reset_enabled`: INT,
          `modem_reset_secs`: BIGINT,
          `internal_err_reboot_enabled`: INT,
          `reboot_secs`: BIGINT,
          `max_random_delay_secs`: BIGINT,
          `sim_switch_requires_modem_resets`: INT,
          `min_modem_resets_for_sim_switch`: BIGINT,
          `connectivity_recovery_logging_enabled`: INT,
          `connectivity_recovery_min_disconnected_secs`: BIGINT
        >,
        `allowed_radio_interfaces_connected_low_power`: ARRAY<
          STRUCT<
            `radio_interface`: INT,
            `enabled`: INT
          >
        >,
        `configure_nv_cat_one_if_enabled_cat_four_if_disabled`: INT,
        `allow_lte_category_nv_changes`: INT,
        `periodic_debug_log_performs_network_scan`: INT,
        `sim_slot_failover_disabled`: INT,
        `use_dsd_for_cell_tech_query`: INT,
        `log_serving_system_info_on_change`: INT,
        `serving_system_info_check_interval_secs`: BIGINT,
        `delay_action`: STRUCT<
          `delay_threshold_ms`: BIGINT,
          `action`: INT,
          `reset_data_call_duration_ms`: BIGINT,
          `cellular_scan_duration_ms`: BIGINT,
          `end_data_call_during_scan`: INT,
          `min_time_between_actions_ms`: BIGINT
        >,
        `forbidden_plmns`: ARRAY<
          STRUCT<
            `mobile_country_code`: BIGINT,
            `mobile_network_code`: BIGINT
          >
        >,
        `network_scan_band_pref_bitmask_override`: DECIMAL(20, 0),
        `lte_call_backoff_in_normal_and_moderate_power_enabled`: INT,
        `dns_servers`: ARRAY<STRING>,
        `startup_sim_probe_enabled`: INT,
        `qmi_panic_reboot`: STRUCT<
          `reboot_on_consecutive_qmi_panic_enabled`: INT,
          `qmi_panic_threshold`: BIGINT,
          `hub_disconnect_threshold_ms`: BIGINT,
          `reboot_while_driving_enabled`: INT
        >,
        `reboot_on_rmnet_missing_enabled`: INT,
        `allow_2g_in_preferred_rat_enabled`: INT,
        `hub_connectivity_based_sim_switching_enabled`: INT,
        `network_scan_on_modem_reset_enabled`: INT,
        `network_scan_timeout_sec`: BIGINT,
        `reset_call_backoff_on_modem_reset_enabled`: INT,
        `sim_slot0_failover_after_ms_v2`: DECIMAL(20, 0),
        `sim_slot1_failover_after_ms_v2`: DECIMAL(20, 0),
        `default_att_apn_for_att_fleet_enabled`: INT,
        `call_restart_on_apn_change_enabled`: INT,
        `qmi_service_handle_missing_reboot`: STRUCT<
          `reboot_on_missing_qmi_service_handle_enabled`: INT,
          `service_handle_retry_count_threshold`: BIGINT
        >
      >,
      `disable_mfr_test_mode`: BOOLEAN,
      `driver_idcard_config`: STRUCT<
        `watch_window_ms`: BIGINT,
        `max_reported_id_cards`: BIGINT,
        `curr_signal_strength_weight`: DOUBLE,
        `idcards`: ARRAY<
          STRUCT<
            `id`: BIGINT,
            `ibeacon`: STRUCT<
              `uuid`: BINARY,
              `major_version`: INT,
              `minor_version`: INT
            >
          >
        >,
        `log_battery_info`: INT,
        `battery_info_report_interval_sec`: BIGINT
      >,
      `gps_harsh_accel_config`: STRUCT<
        `obd_thresh`: STRUCT<
          `accel_mph_per_sec`: FLOAT,
          `brake_mph_per_sec`: FLOAT,
          `cornering_speed_mph`: FLOAT,
          `cornering_degrees_per_sec`: FLOAT
        >,
        `j1939_thresh`: STRUCT<
          `accel_mph_per_sec`: FLOAT,
          `brake_mph_per_sec`: FLOAT,
          `cornering_speed_mph`: FLOAT,
          `cornering_degrees_per_sec`: FLOAT
        >
      >,
      `usb_high_speed`: BOOLEAN,
      `wifi_ap_config`: STRUCT<
        `ssid`: STRING,
        `wpa_passphrase`: STRING,
        `channel`: INT,
        `default_bits_per_second`: BIGINT,
        `throttle_threshold_bytes`: BIGINT,
        `throttle_bits_per_second`: BIGINT,
        `hosts`: ARRAY<
          STRUCT<
            `name`: STRING,
            `block`: BOOLEAN
          >
        >,
        `block_address`: STRING,
        `throttle_tbf_bits_per_second`: BIGINT,
        `throttle_tbf_burst_bytes`: BIGINT,
        `isolate_clients`: BOOLEAN,
        `monthly_quota_metered_bytes`: BIGINT,
        `monthly_quota_grace_bytes`: BIGINT,
        `quota_host_whitelist`: ARRAY<STRING>,
        `old_driver_app_ip_whitelist_sec`: BIGINT,
        `unblock_hotspot_until_unix_sec`: BIGINT,
        `tcp_port_whitelist`: ARRAY<BIGINT>,
        `ignore_broadcast_ssid`: INT,
        `tx_power_dbm`: BIGINT,
        `beacon_int_kus`: BIGINT,
        `enable_old_driver_app_port_whitelist`: INT,
        `hotspot_quota_bypass_udp_port_list`: ARRAY<BIGINT>,
        `target_minimum_encryption`: INT
      >,
      `wifi_mode`: INT,
      `digio_config`: STRUCT<
        `input_sample_period_ms`: INT,
        `analog_input_sample_period_ms`: INT,
        `analog_input_log_min_period_ms`: BIGINT,
        `digi_type_id_1`: INT,
        `camera_event_debounce_ms`: BIGINT,
        `digi_type_id_2`: INT,
        `privacy_button_timeout_secs`: BIGINT,
        `enable_aux_expander`: BOOLEAN,
        `disable_privacy_button_on_engine_start`: BOOLEAN,
        `disable_privacy_mode_on_trip_start`: BOOLEAN,
        `aux_expander_threshold_millivolts`: INT,
        `batch_log_external_voltage_samples`: INT,
        `external_voltage_sample_period_ms`: BIGINT,
        `enable_panic_button_in_low_power`: INT,
        `enable_anio_int_threshold_and_max_period`: INT,
        `complex_input_sample_period_ms`: BIGINT,
        `privacy_button_use`: INT,
        `remote_privacy`: STRUCT<`mode`: INT>,
        `enable_publish_aux_input_signal`: INT,
        `object_stat_collection_timestamp_offset_enabled`: INT,
        `input_configs`: ARRAY<
          STRUCT<
            `input_number`: DECIMAL(20, 0),
            `active_low`: BOOLEAN
          >
        >,
        `external_voltage_batch_report_on_engine_on_transition_enabled`: INT,
        `native_panic_button_wake_enabled`: INT
      >,
      `obd_disable_on_verification_failure`: BOOLEAN,
      `device_i_beacon`: STRUCT<
        `uuid`: BINARY,
        `major_minor`: BIGINT
      >,
      `widget_mgr_problem_timeout_secs`: INT,
      `widget_manufacturing_test_enabled`: BOOLEAN,
      `hub_client`: STRUCT<
        `upgrade_while_moving`: BOOLEAN,
        `desired_cellular_mdm9607_keepalive_interval_seconds`: DECIMAL(20, 0),
        `reboot_on_device_id_change`: INT,
        `compression`: STRUCT<`device_req_compression_enabled`: INT>,
        `c_hub_client_switch_to_golang_hub_client_enabled`: INT
      >,
      `widget_mgr_config`: STRUCT<
        `send_widget_configs`: BOOLEAN,
        `temp_units`: INT,
        `test_enabled`: BOOLEAN,
        `upgrade_before_log_caught_up`: BOOLEAN,
        `report_batch_processing_time`: INT
      >,
      `camera_config`: STRUCT<
        `periodic_report_length_secs`: INT,
        `still_capture_config`: STRUCT<
          `still_capture_interval_secs`: INT,
          `jpeg_settings`: STRUCT<
            `quality`: INT,
            `height_pixels`: INT
          >,
          `inward_jpeg_settings`: STRUCT<
            `quality`: INT,
            `height_pixels`: INT
          >,
          `rear_jpeg_settings`: STRUCT<
            `quality`: INT,
            `height_pixels`: INT
          >,
          `jpeg_encode`: INT,
          `lepton_compress`: INT,
          `primary_camera_config`: STRUCT<
            `trip_start_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_end_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_periodic_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >
          >,
          `secondary_camera_config`: STRUCT<
            `trip_start_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_end_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_periodic_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >
          >,
          `rear_camera_config`: STRUCT<
            `trip_start_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_end_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_periodic_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >
          >,
          `check_stills_needed_period_secs`: BIGINT,
          `start_of_trip_offsets`: STRUCT<
            `start_offset_ms`: BIGINT,
            `end_offset_ms`: BIGINT
          >,
          `still_server_image_capture_enabled`: INT,
          `trip_still_capture`: STRUCT<
            `capture_mode_enabled`: INT,
            `capture_mode`: INT,
            `privacy_filter_enabled`: INT
          >,
          `on_demand_still_capture`: STRUCT<
            `capture_mode_enabled`: INT,
            `capture_mode`: INT,
            `privacy_filter_enabled`: INT
          >,
          `analog1_camera_config`: STRUCT<
            `trip_start_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_end_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_periodic_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >
          >,
          `analog2_camera_config`: STRUCT<
            `trip_start_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_end_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_periodic_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >
          >,
          `analog3_camera_config`: STRUCT<
            `trip_start_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_end_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_periodic_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >
          >,
          `analog4_camera_config`: STRUCT<
            `trip_start_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_end_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >,
            `trip_periodic_stills_config`: STRUCT<
              `enabled`: INT,
              `jpeg_settings`: STRUCT<
                `quality`: INT,
                `height_pixels`: INT
              >
            >
          >
        >,
        `persistent_storage`: STRUCT<
          `max_percentage_to_store`: INT,
          `high_res_storage_percent`: BIGINT,
          `media_retention_days`: BIGINT,
          `high_res_external_storage_percent`: BIGINT
        >,
        `stop_delay_ms`: BIGINT,
        `camera_flags`: INT,
        `clip_write_buffer_size`: DECIMAL(20, 0),
        `disable_write_durations`: BOOLEAN,
        `long_segment_length_secs`: BIGINT,
        `audio_sample_rate`: DECIMAL(20, 0),
        `enable_vision_manager`: BOOLEAN,
        `dashcam_connected`: BOOLEAN,
        `enable_startup_sounds`: BOOLEAN,
        `panic_button_report_length_secs`: INT,
        `disable_auto_upload_regular_harsh_events`: BOOLEAN,
        `disable_auto_upload_adas_harsh_events`: BOOLEAN,
        `primary_fps`: BIGINT,
        `secondary_fps`: BIGINT,
        `rear_camera_config`: STRUCT<
          `bitrate`: BIGINT,
          `framerate`: BIGINT,
          `codec`: INT,
          `resolution`: INT,
          `bitrate_control`: INT,
          `idr_interval_seconds`: INT,
          `video_transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >,
          `bframes_m_value`: INT
        >,
        `primary_high_res_config`: STRUCT<
          `bitrate`: BIGINT,
          `framerate`: BIGINT,
          `codec`: INT,
          `resolution`: INT,
          `bitrate_control`: INT,
          `idr_interval_seconds`: INT,
          `video_transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >,
          `bframes_m_value`: INT
        >,
        `secondary_high_res_config`: STRUCT<
          `bitrate`: BIGINT,
          `framerate`: BIGINT,
          `codec`: INT,
          `resolution`: INT,
          `bitrate_control`: INT,
          `idr_interval_seconds`: INT,
          `video_transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >,
          `bframes_m_value`: INT
        >,
        `primary_low_res_config`: STRUCT<
          `bitrate`: BIGINT,
          `framerate`: BIGINT,
          `codec`: INT,
          `resolution`: INT,
          `bitrate_control`: INT,
          `idr_interval_seconds`: INT,
          `video_transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >,
          `bframes_m_value`: INT
        >,
        `secondary_low_res_config`: STRUCT<
          `bitrate`: BIGINT,
          `framerate`: BIGINT,
          `codec`: INT,
          `resolution`: INT,
          `bitrate_control`: INT,
          `idr_interval_seconds`: INT,
          `video_transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >,
          `bframes_m_value`: INT
        >,
        `parking_mode_config`: STRUCT<
          `enabled`: INT,
          `duration_secs`: BIGINT,
          `primary_high_res_fps`: BIGINT,
          `secondary_high_res_fps`: BIGINT,
          `primary_low_res_fps`: BIGINT,
          `secondary_low_res_fps`: BIGINT,
          `primary_high_res`: INT,
          `secondary_high_res`: INT,
          `primary_low_res`: INT,
          `secondary_low_res`: INT,
          `microphone`: INT,
          `rear_camera`: INT,
          `idle_recording_enabled`: INT,
          `input_recording`: STRUCT<
            `primary`: STRUCT<
              `high_res`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `bitrate`: BIGINT,
                  `framerate`: BIGINT,
                  `codec`: INT,
                  `resolution`: INT,
                  `bitrate_control`: INT,
                  `idr_interval_seconds`: INT,
                  `video_transform`: STRUCT<
                    `flip_vertical`: BOOLEAN,
                    `flip_horizontal`: BOOLEAN,
                    `rotation`: INT
                  >,
                  `bframes_m_value`: INT
                >
              >,
              `low_res`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `bitrate`: BIGINT,
                  `framerate`: BIGINT,
                  `codec`: INT,
                  `resolution`: INT,
                  `bitrate_control`: INT,
                  `idr_interval_seconds`: INT,
                  `video_transform`: STRUCT<
                    `flip_vertical`: BOOLEAN,
                    `flip_horizontal`: BOOLEAN,
                    `rotation`: INT
                  >,
                  `bframes_m_value`: INT
                >
              >,
              `audio`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `samplerate`: BIGINT,
                  `audio_codec`: INT
                >
              >
            >,
            `secondary`: STRUCT<
              `high_res`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `bitrate`: BIGINT,
                  `framerate`: BIGINT,
                  `codec`: INT,
                  `resolution`: INT,
                  `bitrate_control`: INT,
                  `idr_interval_seconds`: INT,
                  `video_transform`: STRUCT<
                    `flip_vertical`: BOOLEAN,
                    `flip_horizontal`: BOOLEAN,
                    `rotation`: INT
                  >,
                  `bframes_m_value`: INT
                >
              >,
              `low_res`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `bitrate`: BIGINT,
                  `framerate`: BIGINT,
                  `codec`: INT,
                  `resolution`: INT,
                  `bitrate_control`: INT,
                  `idr_interval_seconds`: INT,
                  `video_transform`: STRUCT<
                    `flip_vertical`: BOOLEAN,
                    `flip_horizontal`: BOOLEAN,
                    `rotation`: INT
                  >,
                  `bframes_m_value`: INT
                >
              >,
              `audio`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `samplerate`: BIGINT,
                  `audio_codec`: INT
                >
              >
            >,
            `analog_1`: STRUCT<
              `high_res`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `bitrate`: BIGINT,
                  `framerate`: BIGINT,
                  `codec`: INT,
                  `resolution`: INT,
                  `bitrate_control`: INT,
                  `idr_interval_seconds`: INT,
                  `video_transform`: STRUCT<
                    `flip_vertical`: BOOLEAN,
                    `flip_horizontal`: BOOLEAN,
                    `rotation`: INT
                  >,
                  `bframes_m_value`: INT
                >
              >,
              `low_res`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `bitrate`: BIGINT,
                  `framerate`: BIGINT,
                  `codec`: INT,
                  `resolution`: INT,
                  `bitrate_control`: INT,
                  `idr_interval_seconds`: INT,
                  `video_transform`: STRUCT<
                    `flip_vertical`: BOOLEAN,
                    `flip_horizontal`: BOOLEAN,
                    `rotation`: INT
                  >,
                  `bframes_m_value`: INT
                >
              >,
              `audio`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `samplerate`: BIGINT,
                  `audio_codec`: INT
                >
              >
            >,
            `analog_2`: STRUCT<
              `high_res`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `bitrate`: BIGINT,
                  `framerate`: BIGINT,
                  `codec`: INT,
                  `resolution`: INT,
                  `bitrate_control`: INT,
                  `idr_interval_seconds`: INT,
                  `video_transform`: STRUCT<
                    `flip_vertical`: BOOLEAN,
                    `flip_horizontal`: BOOLEAN,
                    `rotation`: INT
                  >,
                  `bframes_m_value`: INT
                >
              >,
              `low_res`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `bitrate`: BIGINT,
                  `framerate`: BIGINT,
                  `codec`: INT,
                  `resolution`: INT,
                  `bitrate_control`: INT,
                  `idr_interval_seconds`: INT,
                  `video_transform`: STRUCT<
                    `flip_vertical`: BOOLEAN,
                    `flip_horizontal`: BOOLEAN,
                    `rotation`: INT
                  >,
                  `bframes_m_value`: INT
                >
              >,
              `audio`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `samplerate`: BIGINT,
                  `audio_codec`: INT
                >
              >
            >,
            `analog_3`: STRUCT<
              `high_res`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `bitrate`: BIGINT,
                  `framerate`: BIGINT,
                  `codec`: INT,
                  `resolution`: INT,
                  `bitrate_control`: INT,
                  `idr_interval_seconds`: INT,
                  `video_transform`: STRUCT<
                    `flip_vertical`: BOOLEAN,
                    `flip_horizontal`: BOOLEAN,
                    `rotation`: INT
                  >,
                  `bframes_m_value`: INT
                >
              >,
              `low_res`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `bitrate`: BIGINT,
                  `framerate`: BIGINT,
                  `codec`: INT,
                  `resolution`: INT,
                  `bitrate_control`: INT,
                  `idr_interval_seconds`: INT,
                  `video_transform`: STRUCT<
                    `flip_vertical`: BOOLEAN,
                    `flip_horizontal`: BOOLEAN,
                    `rotation`: INT
                  >,
                  `bframes_m_value`: INT
                >
              >,
              `audio`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `samplerate`: BIGINT,
                  `audio_codec`: INT
                >
              >
            >,
            `analog_4`: STRUCT<
              `high_res`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `bitrate`: BIGINT,
                  `framerate`: BIGINT,
                  `codec`: INT,
                  `resolution`: INT,
                  `bitrate_control`: INT,
                  `idr_interval_seconds`: INT,
                  `video_transform`: STRUCT<
                    `flip_vertical`: BOOLEAN,
                    `flip_horizontal`: BOOLEAN,
                    `rotation`: INT
                  >,
                  `bframes_m_value`: INT
                >
              >,
              `low_res`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `bitrate`: BIGINT,
                  `framerate`: BIGINT,
                  `codec`: INT,
                  `resolution`: INT,
                  `bitrate_control`: INT,
                  `idr_interval_seconds`: INT,
                  `video_transform`: STRUCT<
                    `flip_vertical`: BOOLEAN,
                    `flip_horizontal`: BOOLEAN,
                    `rotation`: INT
                  >,
                  `bframes_m_value`: INT
                >
              >,
              `audio`: STRUCT<
                `mode`: INT,
                `info`: STRUCT<
                  `samplerate`: BIGINT,
                  `audio_codec`: INT
                >
              >
            >
          >
        >,
        `use_ecu_speed_and_gps_from_msg_bus`: INT,
        `primary_low_res_stream_v2`: INT,
        `secondary_low_res_stream_v2`: INT,
        `enable_rear_no_signal_recording`: INT,
        `message_bus_archiver_config`: STRUCT<
          `enable_deprecated`: INT,
          `denied_message_types`: ARRAY<BIGINT>,
          `enable`: INT
        >,
        `record_on_trip`: INT,
        `qcs603_frameserver_config`: STRUCT<
          `qmmf_stability_config`: STRUCT<
            `start_delays_enabled`: INT,
            `delay_track_ms`: BIGINT,
            `delay_fps_ms`: BIGINT,
            `buffer_wait_timeout_offset_ms`: BIGINT
          >,
          `non_blocking`: STRUCT<
            `enabled`: INT,
            `max_encoder_input_queue_len`: BIGINT,
            `extra_buffer_count`: BIGINT
          >,
          `thermal_mode_overheat2`: STRUCT<
            `fps_cap`: BIGINT,
            `bitrate_cap`: BIGINT,
            `fps_cap_valid`: BOOLEAN,
            `bitrate_cap_valid`: BOOLEAN
          >,
          `thermal_mode_safemode`: STRUCT<
            `fps_cap`: BIGINT,
            `bitrate_cap`: BIGINT,
            `fps_cap_valid`: BOOLEAN,
            `bitrate_cap_valid`: BOOLEAN
          >
        >,
        `qcs603_config`: STRUCT<
          `emergency_downloader_mode_usb_reset_enabled`: INT,
          `emergency_downloader_mode_usb_reset_interval_ms`: BIGINT,
          `emergency_downloader_mode_usb_reset_duration_ms`: BIGINT,
          `emergency_downloader_mode_usb_reset_max_times`: BIGINT
        >,
        `report_config`: STRUCT<
          `boot_count_before_timeout_ms`: BIGINT,
          `audio_writer_padding_enabled`: INT,
          `enable_handling_video_retrieval_spanning_reboots`: INT,
          `audio_writer_padding_gap_threshold_ms`: DECIMAL(20, 0),
          `mp4_writer_b_frame_support_enabled`: INT
        >,
        `message_bus_proxy_binary_codec_enabled`: INT,
        `audio_encode_enabled`: INT,
        `microphone_config`: STRUCT<
          `samplerate`: BIGINT,
          `audio_codec`: INT
        >,
        `recording_with_recording_manager_enabled`: INT,
        `gps_time_update_enabled`: INT,
        `recording_manager_merge_with_vg_logic_enabled`: INT,
        `input_recording`: STRUCT<
          `primary`: STRUCT<
            `high_res`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `bitrate`: BIGINT,
                `framerate`: BIGINT,
                `codec`: INT,
                `resolution`: INT,
                `bitrate_control`: INT,
                `idr_interval_seconds`: INT,
                `video_transform`: STRUCT<
                  `flip_vertical`: BOOLEAN,
                  `flip_horizontal`: BOOLEAN,
                  `rotation`: INT
                >,
                `bframes_m_value`: INT
              >
            >,
            `low_res`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `bitrate`: BIGINT,
                `framerate`: BIGINT,
                `codec`: INT,
                `resolution`: INT,
                `bitrate_control`: INT,
                `idr_interval_seconds`: INT,
                `video_transform`: STRUCT<
                  `flip_vertical`: BOOLEAN,
                  `flip_horizontal`: BOOLEAN,
                  `rotation`: INT
                >,
                `bframes_m_value`: INT
              >
            >,
            `audio`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `samplerate`: BIGINT,
                `audio_codec`: INT
              >
            >
          >,
          `secondary`: STRUCT<
            `high_res`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `bitrate`: BIGINT,
                `framerate`: BIGINT,
                `codec`: INT,
                `resolution`: INT,
                `bitrate_control`: INT,
                `idr_interval_seconds`: INT,
                `video_transform`: STRUCT<
                  `flip_vertical`: BOOLEAN,
                  `flip_horizontal`: BOOLEAN,
                  `rotation`: INT
                >,
                `bframes_m_value`: INT
              >
            >,
            `low_res`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `bitrate`: BIGINT,
                `framerate`: BIGINT,
                `codec`: INT,
                `resolution`: INT,
                `bitrate_control`: INT,
                `idr_interval_seconds`: INT,
                `video_transform`: STRUCT<
                  `flip_vertical`: BOOLEAN,
                  `flip_horizontal`: BOOLEAN,
                  `rotation`: INT
                >,
                `bframes_m_value`: INT
              >
            >,
            `audio`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `samplerate`: BIGINT,
                `audio_codec`: INT
              >
            >
          >,
          `analog_1`: STRUCT<
            `high_res`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `bitrate`: BIGINT,
                `framerate`: BIGINT,
                `codec`: INT,
                `resolution`: INT,
                `bitrate_control`: INT,
                `idr_interval_seconds`: INT,
                `video_transform`: STRUCT<
                  `flip_vertical`: BOOLEAN,
                  `flip_horizontal`: BOOLEAN,
                  `rotation`: INT
                >,
                `bframes_m_value`: INT
              >
            >,
            `low_res`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `bitrate`: BIGINT,
                `framerate`: BIGINT,
                `codec`: INT,
                `resolution`: INT,
                `bitrate_control`: INT,
                `idr_interval_seconds`: INT,
                `video_transform`: STRUCT<
                  `flip_vertical`: BOOLEAN,
                  `flip_horizontal`: BOOLEAN,
                  `rotation`: INT
                >,
                `bframes_m_value`: INT
              >
            >,
            `audio`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `samplerate`: BIGINT,
                `audio_codec`: INT
              >
            >
          >,
          `analog_2`: STRUCT<
            `high_res`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `bitrate`: BIGINT,
                `framerate`: BIGINT,
                `codec`: INT,
                `resolution`: INT,
                `bitrate_control`: INT,
                `idr_interval_seconds`: INT,
                `video_transform`: STRUCT<
                  `flip_vertical`: BOOLEAN,
                  `flip_horizontal`: BOOLEAN,
                  `rotation`: INT
                >,
                `bframes_m_value`: INT
              >
            >,
            `low_res`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `bitrate`: BIGINT,
                `framerate`: BIGINT,
                `codec`: INT,
                `resolution`: INT,
                `bitrate_control`: INT,
                `idr_interval_seconds`: INT,
                `video_transform`: STRUCT<
                  `flip_vertical`: BOOLEAN,
                  `flip_horizontal`: BOOLEAN,
                  `rotation`: INT
                >,
                `bframes_m_value`: INT
              >
            >,
            `audio`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `samplerate`: BIGINT,
                `audio_codec`: INT
              >
            >
          >,
          `analog_3`: STRUCT<
            `high_res`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `bitrate`: BIGINT,
                `framerate`: BIGINT,
                `codec`: INT,
                `resolution`: INT,
                `bitrate_control`: INT,
                `idr_interval_seconds`: INT,
                `video_transform`: STRUCT<
                  `flip_vertical`: BOOLEAN,
                  `flip_horizontal`: BOOLEAN,
                  `rotation`: INT
                >,
                `bframes_m_value`: INT
              >
            >,
            `low_res`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `bitrate`: BIGINT,
                `framerate`: BIGINT,
                `codec`: INT,
                `resolution`: INT,
                `bitrate_control`: INT,
                `idr_interval_seconds`: INT,
                `video_transform`: STRUCT<
                  `flip_vertical`: BOOLEAN,
                  `flip_horizontal`: BOOLEAN,
                  `rotation`: INT
                >,
                `bframes_m_value`: INT
              >
            >,
            `audio`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `samplerate`: BIGINT,
                `audio_codec`: INT
              >
            >
          >,
          `analog_4`: STRUCT<
            `high_res`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `bitrate`: BIGINT,
                `framerate`: BIGINT,
                `codec`: INT,
                `resolution`: INT,
                `bitrate_control`: INT,
                `idr_interval_seconds`: INT,
                `video_transform`: STRUCT<
                  `flip_vertical`: BOOLEAN,
                  `flip_horizontal`: BOOLEAN,
                  `rotation`: INT
                >,
                `bframes_m_value`: INT
              >
            >,
            `low_res`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `bitrate`: BIGINT,
                `framerate`: BIGINT,
                `codec`: INT,
                `resolution`: INT,
                `bitrate_control`: INT,
                `idr_interval_seconds`: INT,
                `video_transform`: STRUCT<
                  `flip_vertical`: BOOLEAN,
                  `flip_horizontal`: BOOLEAN,
                  `rotation`: INT
                >,
                `bframes_m_value`: INT
              >
            >,
            `audio`: STRUCT<
              `mode`: INT,
              `info`: STRUCT<
                `samplerate`: BIGINT,
                `audio_codec`: INT
              >
            >
          >
        >,
        `clip`: STRUCT<
          `version`: INT,
          `periodic_sync`: STRUCT<
            `enabled`: INT,
            `interval_ms`: DECIMAL(20, 0),
            `fallocate_size_mb`: BIGINT
          >
        >,
        `recording_data_only_state_enabled`: INT,
        `log_recording_status_with_state_enabled`: INT,
        `frame_buffer_depth`: BIGINT,
        `multiplex_av_enabled`: INT,
        `event_based_recording_config`: STRUCT<
          `pre_event_recording_ms`: BIGINT,
          `post_event_recording_ms`: BIGINT,
          `store_recordings_for_shadow_mode_harsh_events_enabled`: INT,
          `store_recordings_on_abrupt_shutdown_enabled`: INT
        >,
        `secondary_extended_audio_enabled`: INT
      >,
      `uploader_config`: STRUCT<
        `video_recall_requests`: ARRAY<
          STRUCT<
            `time_ms`: BIGINT,
            `length_ms`: BIGINT,
            `request_id`: BIGINT,
            `get_every_nth_keyframe`: BIGINT,
            `slowdown_factor`: BIGINT,
            `multicam_requests`: ARRAY<
              STRUCT<
                `track_id`: BIGINT,
                `camera_id`: BIGINT
              >
            >,
            `wifi_required`: BOOLEAN,
            `low_res_only`: BOOLEAN
          >
        >,
        `misc_file_components`: ARRAY<
          STRUCT<`name`: STRING>
        >,
        `dashcam_reports`: ARRAY<
          STRUCT<
            `event_id`: BIGINT,
            `unix_trigger_time_ms`: BIGINT,
            `trigger_reason`: INT,
            `report_type`: INT,
            `start_time`: BIGINT,
            `end_time`: BIGINT,
            `start_offset`: BIGINT,
            `end_offset`: BIGINT,
            `camera_type`: INT,
            `gateway_id`: BIGINT,
            `multicam_config`: STRUCT<
              `request_cameras`: ARRAY<
                STRUCT<
                  `camera_device_id`: BIGINT,
                  `stream_id`: BIGINT
                >
              >
            >,
            `streams`: ARRAY<INT>,
            `wifi_required`: BOOLEAN,
            `camera_still_info`: STRUCT<
              `jpeg_encode`: INT,
              `camera_still_settings`: ARRAY<
                STRUCT<
                  `stream`: INT,
                  `jpeg_settings`: STRUCT<
                    `quality`: INT,
                    `height_pixels`: INT
                  >,
                  `media_stream_id`: STRUCT<
                    `input`: INT,
                    `stream`: INT
                  >
                >
              >
            >,
            `media_stream_ids`: ARRAY<
              STRUCT<
                `input`: INT,
                `stream`: INT
              >
            >,
            `data_stream_ids`: ARRAY<INT>,
            `location_decorations`: ARRAY<
              STRUCT<
                `latitude_microdegrees`: BIGINT,
                `longitude_microdegrees`: BIGINT,
                `horizontal_uncertainty_valid`: BOOLEAN,
                `horizontal_uncertainty_millimeters`: BIGINT,
                `altitude_valid`: BOOLEAN,
                `mean_sea_level_altitude_millimeters`: BIGINT,
                `altitude_uncertainty_valid`: BOOLEAN,
                `altitude_uncertainty_millimeters`: BIGINT,
                `gnss_speed_valid`: BOOLEAN,
                `gnss_speed_millimeters_per_second`: BIGINT,
                `gnss_speed_uncertainty_valid`: BOOLEAN,
                `gnss_speed_uncertainty_millimeters_per_second`: BIGINT,
                `heading_valid`: BOOLEAN,
                `heading_millidegrees`: BIGINT,
                `heading_uncertainty_valid`: BOOLEAN,
                `heading_uncertainty_millidegrees`: BIGINT,
                `time_offset_from_decorated_stat_ms`: BIGINT
              >
            >
          >
        >,
        `num_parallel_uploads`: BIGINT
      >,
      `accel_mgr_config`: STRUCT<
        `dump_accel`: BOOLEAN,
        `accel_crash_detect_config`: STRUCT<
          `crash_threshold_gs`: DOUBLE,
          `crash_window_size_ms`: INT,
          `window_subset_for_crash_ms`: INT,
          `test_mode`: BOOLEAN,
          `save_local_log`: BOOLEAN
        >,
        `accel_harsh_detect_config`: STRUCT<
          `brake_threshold_gs`: DOUBLE,
          `harsh_window_size_ms`: INT,
          `window_subset_for_harsh_ms`: INT,
          `test_mode`: BOOLEAN,
          `save_local_log`: BOOLEAN,
          `normal_turning_rke_threshold`: DOUBLE,
          `event_minimum_milliknots`: BIGINT
        >,
        `dump_gyro`: BOOLEAN,
        `j1939_harsh_detect_config`: STRUCT<
          `brake_threshold_gs`: DOUBLE,
          `harsh_window_size_ms`: INT,
          `window_subset_for_harsh_ms`: INT,
          `test_mode`: BOOLEAN,
          `save_local_log`: BOOLEAN,
          `normal_turning_rke_threshold`: DOUBLE,
          `event_minimum_milliknots`: BIGINT
        >,
        `accel_mode`: INT,
        `vibration_sampling_interval_ms`: BIGINT,
        `j1939_crash_detect_config`: STRUCT<
          `crash_threshold_gs`: DOUBLE,
          `crash_window_size_ms`: INT,
          `window_subset_for_crash_ms`: INT,
          `test_mode`: BOOLEAN,
          `save_local_log`: BOOLEAN
        >,
        `disabled`: BOOLEAN,
        `oriented_harsh_event_config`: STRUCT<
          `enable`: BOOLEAN,
          `harsh_accel_x_threshold_gs`: FLOAT,
          `harsh_brake_x_threshold_gs`: FLOAT,
          `harsh_turn_x_threshold_gs`: FLOAT,
          `accel_window_size_ms`: BIGINT,
          `median_filter_span_ms`: BIGINT,
          `ewma_filter_span_ms`: BIGINT,
          `debug_mode`: BOOLEAN,
          `disable`: BOOLEAN,
          `debug_mode_past_12_1`: BOOLEAN
        >,
        `imu_orienter_config`: STRUCT<
          `downsample_factor`: BIGINT,
          `gravity_median_accel_span_ms`: BIGINT,
          `gravity_ewma_accel_span_ms`: BIGINT,
          `multiple_orientations_rotation_threshold_rads`: DOUBLE,
          `gravity_min_mag_gs`: DOUBLE,
          `gravity_max_mag_gs`: DOUBLE,
          `gravity_vector_cache_size`: BIGINT,
          `gravity_orientation_sample_size_ms`: BIGINT,
          `gravity_orientation_interval_ms`: BIGINT,
          `yaw_orientation_sample_size_ms`: BIGINT,
          `yaw_angle_cache_size`: BIGINT,
          `yaw_orientation_interval_ms`: BIGINT,
          `yaw_optimizer`: STRUCT<
            `convergence_threshold`: FLOAT,
            `max_iterations`: BIGINT,
            `max_runtime_ms`: BIGINT
          >,
          `yaw_ewma_filter_span_ms`: BIGINT,
          `yaw_confidence_minimum`: DOUBLE,
          `min_yaw_sample_accel_magnitude_gs`: DOUBLE,
          `min_yaw_sample_exceeding_magnitude_count`: BIGINT,
          `gravity_vector_sample_weight_max`: DOUBLE,
          `gravity_vector_sample_weight_min`: DOUBLE,
          `gravity_vector_sample_weight_decay`: DOUBLE,
          `yaw_angle_sample_weight_max`: DOUBLE,
          `yaw_angle_sample_weight_min`: DOUBLE,
          `yaw_angle_sample_weight_decay`: DOUBLE,
          `disable`: BOOLEAN,
          `disable_past_10_30_3`: BOOLEAN
        >,
        `verbose_accel_logging`: BOOLEAN,
        `disable_verbose_logging`: BOOLEAN,
        `customer_visible_ingestion_tag`: DECIMAL(20, 0),
        `enable_harsh_event_v2`: INT,
        `imu_calibration_config`: STRUCT<
          `calibration_reset`: STRUCT<
            `sent_at_unix_ms`: DECIMAL(20, 0),
            `reset_tag`: BIGINT
          >,
          `sample_collection`: STRUCT<
            `strong_turn_min_gyro_norm_dps`: INT,
            `no_turn_max_gyro_norm_dps`: INT,
            `save_progress_interval_secs`: BIGINT
          >,
          `gyroscope_bias`: STRUCT<
            `gyro_at_rest_max_norm_dps`: BIGINT,
            `gyro_at_rest_max_gyro_slope_filter_dps`: FLOAT,
            `gyro_at_rest_max_accel_slope_filter_g`: FLOAT,
            `gyro_bias_max_valid_value_dps`: BIGINT,
            `gyro_bias_update_min_change_dps`: FLOAT
          >,
          `short_term_gravity`: STRUCT<
            `accel_samples_max_offset_from_1g`: FLOAT,
            `lock_sample_count`: BIGINT,
            `lock_max_std_dev_g`: FLOAT,
            `orientation_change_threshold_degrees`: BIGINT
          >,
          `gravity`: STRUCT<
            `accel_samples_max_offset_from_1g`: FLOAT,
            `lock_sample_count`: BIGINT
          >,
          `yaw_angle`: STRUCT<
            `left_right_turn_gyro_z_axis_threshold_dps`: INT,
            `left_right_turn_min_accel_mag_g`: FLOAT,
            `strong_accel_brake_min_accel_mag_g`: FLOAT,
            `strong_accel_brake_max_z_axis_g`: FLOAT,
            `lock_min_left_and_right_sample_count`: BIGINT,
            `lock_min_strong_accel_or_brake_sample_count`: BIGINT,
            `lock_validity_max_forward_side_dot_product`: FLOAT,
            `lock_validity_min_left_right_angle_degrees`: BIGINT
          >,
          `orientation_updates`: STRUCT<
            `min_gravity_change_degrees`: BIGINT,
            `min_yaw_angle_change_degrees`: BIGINT
          >
        >,
        `accel_event_data_trace_replay_config`: STRUCT<
          `enabled`: INT,
          `trace_file`: STRING,
          `loop_trace`: BOOLEAN,
          `playback_speed`: BIGINT
        >,
        `imu_harsh_event_config`: STRUCT<
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
        `orientation_override`: STRUCT<
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
        `enable_harsh_event_v1`: INT,
        `enable_imu_cal_v2`: INT,
        `enable_auto_hev1_hev2_switch`: INT,
        `enable_imu_cal_persist_progress_deprecated`: INT,
        `enable_log_hev2_events_below_customer_threshold`: INT,
        `enable_raw_imu_data_publishing`: INT,
        `enable_batch_log_accel_mag_samples`: INT,
        `enable_imu_cal_persist_progress`: INT,
        `enable_oriented_imu_data_publishing`: INT,
        `async_imu_data_reading_enabled`: INT
      >,
      `usb_config`: STRUCT<
        `dump_usbmon`: BOOLEAN,
        `disabled`: BOOLEAN,
        `peripheral_speed`: INT,
        `allow_usb_on_battery`: BOOLEAN,
        `modi_thermal_shutoff`: STRUCT<
          `power_off_threshold_milli_c`: BIGINT,
          `power_on_threshold_milli_c`: BIGINT,
          `sample_period_ms`: BIGINT,
          `window_num_samples`: BIGINT,
          `allow_power_to_single_port_for_ms`: DECIMAL(20, 0)
        >,
        `host_config_in_moderate_power`: INT,
        `keep_mcu_in_full_power_when_usb_hub_powered`: INT,
        `thor_usb_hub_health_checks_config`: STRUCT<
          `hub_missing_check_enabled`: INT,
          `hub_missing_check_interval_ms`: BIGINT,
          `hub_missing_grace_period_ms`: BIGINT,
          `mitigate_stuck_primary_buck_hardware_version_allowlist`: ARRAY<STRING>,
          `stuck_primary_buck_min_recovery_interval_ms`: BIGINT
        >,
        `activate_mdm9607_root_hub_before_hub_chip`: INT
      >,
      `public_web_server`: STRUCT<
        `max_eld_events_buffer_length`: BIGINT,
        `usb_clients_enabled`: BOOLEAN,
        `blink_led_when_moving_and_driver_not_signed_in`: BOOLEAN,
        `beep_when_moving_and_driver_not_signed_in`: INT,
        `restart_process_on_http_server_error`: INT
      >,
      `local_config_override`: STRING,
      `ble_scanner`: STRUCT<
        `no_adv_timeout_secs`: BIGINT,
        `no_adv_reboot_threshold`: BIGINT,
        `disabled`: BOOLEAN,
        `enable_concurrent_upgrades`: BOOLEAN,
        `disable_upgrades`: BOOLEAN,
        `adv_queue_timeout_ms`: DECIMAL(20, 0),
        `enable_widget_connection_logging`: BOOLEAN,
        `new_image_file`: STRING,
        `product_shortname_uses_new_image`: ARRAY<STRING>,
        `report_advertising_statistics`: INT,
        `advertisement_stale_after_ms`: BIGINT,
        `advertisement_stats_max_window_ms`: BIGINT,
        `report_log_statistics`: INT,
        `log_stats_max_window_ms`: BIGINT,
        `initial`: STRUCT<
          `interval_min_ms`: BIGINT,
          `interval_max_ms`: BIGINT,
          `peripheral_latency`: BIGINT,
          `supervisory_timeout_ms`: BIGINT
        >,
        `caught_up`: STRUCT<
          `interval_min_ms`: BIGINT,
          `interval_max_ms`: BIGINT,
          `peripheral_latency`: BIGINT,
          `supervisory_timeout_ms`: BIGINT
        >,
        `log_widget_adv_stats`: INT,
        `widget_adv_stats_log_period_ms`: BIGINT,
        `require_directed_or_broadcast_adv_stats`: INT,
        `crux`: STRUCT<
          `central`: STRUCT<
            `enable`: INT,
            `max_log_period_ms`: BIGINT,
            `max_peripherals_per_log_period`: BIGINT,
            `log_factory_stats_enable`: INT,
            `log_experimental_fce6_uuid_stats_enable`: INT,
            `min_time_between_connecting_to_peripherals_seconds`: BIGINT,
            `connect_to_peripherals_enable`: INT,
            `configure_peripherals_enable`: INT,
            `dfu_peripherals_enable`: INT,
            `log_ble_proxy_adv_stats_enable`: INT,
            `max_bleproxy_peripherals_per_log_period`: BIGINT,
            `log_experimental_fce4_uuid_stats_enable`: INT,
            `log_fc87_uuid_stats_enable`: INT,
            `log_fc86_uuid_stats_enable`: INT,
            `prioritize_fc87_uuid_advertisements`: INT
          >
        >,
        `enable_wait_for_connection_update_complete`: INT,
        `connection_update_complete_timeout_ms`: BIGINT,
        `dynamic_allowlist_config`: STRUCT<
          `monitor_enabled`: INT,
          `crux_enabled`: INT,
          `min_query_interval_seconds`: BIGINT,
          `max_peripherals_per_device_request`: BIGINT
        >,
        `conn_failure_recovery_enable`: INT,
        `max_forkbeard_peripherals_per_log_period`: BIGINT,
        `bifrost`: STRUCT<
          `central`: STRUCT<
            `connect_to_peripherals_enable`: INT,
            `configure_peripherals_enable`: INT,
            `dfu_peripherals_enable`: INT,
            `upload_low_priority_data_enable`: INT,
            `min_time_between_connecting_to_altus_seconds`: BIGINT,
            `min_time_between_connecting_to_vanguard_seconds`: BIGINT,
            `data_upload_disable`: INT,
            `min_time_between_connecting_to_superhanselito_seconds`: BIGINT,
            `dfu_allow_downgrades`: INT
          >
        >
      >,
      `group_config_override`: STRING,
      `log_manager`: STRUCT<
        `max_logs_per_push`: DECIMAL(20, 0),
        `disable_upload`: BOOLEAN,
        `forward_to_public_web_server`: BOOLEAN,
        `deletion_event_enabled`: INT,
        `enable_upload_in_low_power`: INT,
        `enable_log_event_tracking`: INT,
        `enable_disk_size_limit`: INT,
        `num_logs_to_delete_on_disk_full`: DECIMAL(20, 0),
        `blocked_stat_enum_lists`: ARRAY<
          STRUCT<
            `applicable_build`: STRING,
            `blocked_stat_enums`: ARRAY<INT>
          >
        >,
        `excessive_obj_stat_reporting`: STRUCT<
          `period_mins`: BIGINT,
          `count_limit`: BIGINT
        >,
        `log_event_upload_delay`: STRUCT<
          `enable`: INT,
          `delay_ms`: BIGINT,
          `immediately_upload_device_location`: INT,
          `immediately_upload_widget_stats`: INT,
          `object_stats`: ARRAY<
            STRUCT<
              `stat`: INT,
              `immediately_upload`: INT
            >
          >
        >,
        `max_logs_per_db_insert`: DECIMAL(20, 0),
        `time_source_prioritization_enabled`: INT,
        `max_db_size_bytes`: DECIMAL(20, 0),
        `vacuum`: STRUCT<
          `vacuum_when_over_max_db_size_enabled`: INT,
          `report_db_info_enabled`: INT,
          `min_reclaimable_bytes_to_vacuum`: DECIMAL(20, 0)
        >,
        `gps_time_lock`: STRUCT<
          `lock_source_enabled`: INT,
          `lock_delay_secs`: BIGINT
        >
      >,
      `audio_alerts`: STRUCT<
        `language`: INT,
        `alert_flags`: INT,
        `playback_deprecated`: STRUCT<
          `speaker_volume`: BIGINT,
          `log_alert_play`: BOOLEAN,
          `alert_logging_enabled`: INT
        >,
        `audio_alert_configs`: ARRAY<
          STRUCT<
            `event_type`: INT,
            `severity`: INT,
            `enabled`: BOOLEAN,
            `audio_file_path`: STRING,
            `volume`: BIGINT,
            `priority`: BIGINT,
            `repeat_config`: STRUCT<
              `enabled`: BOOLEAN,
              `count`: BIGINT,
              `interval_ms`: BIGINT
            >,
            `is_voiceless`: BOOLEAN
          >
        >,
        `audio_alert_configs_overrides`: ARRAY<
          STRUCT<
            `event_type`: INT,
            `severity`: INT,
            `enabled`: BOOLEAN,
            `audio_file_path`: STRING,
            `volume`: BIGINT,
            `priority`: BIGINT,
            `repeat_config`: STRUCT<
              `enabled`: BOOLEAN,
              `count`: BIGINT,
              `interval_ms`: BIGINT
            >,
            `is_voiceless`: BOOLEAN
          >
        >,
        `playback`: STRUCT<
          `speaker_volume`: BIGINT,
          `log_alert_play`: BOOLEAN,
          `alert_logging_enabled`: INT
        >,
        `qcs603`: STRUCT<`audio_player`: INT>,
        `vg`: STRUCT<`play_alerts_on_banshee_and_cm_enabled`: INT>,
        `audio_alert_policy`: STRUCT<
          `sliding_window_rate_limit_rules`: ARRAY<
            STRUCT<
              `rule_enabled`: INT,
              `window_ms`: BIGINT,
              `max_alert_count`: BIGINT,
              `event_type`: INT,
              `cloud_trigger_reason`: INT
            >
          >,
          `priority_category_to_priority_mapping_configs`: ARRAY<
            STRUCT<
              `priority_category`: INT,
              `priority`: BIGINT
            >
          >,
          `per_event_configs`: ARRAY<
            STRUCT<
              `event_type`: INT,
              `priority_category_config`: STRUCT<
                `enabled`: INT,
                `priority_category`: INT
              >,
              `per_severity_configs`: ARRAY<
                STRUCT<
                  `severity`: INT,
                  `repeat_config`: STRUCT<
                    `enabled`: INT,
                    `repeat_count`: BIGINT,
                    `unlimited_repeat_count`: BOOLEAN,
                    `repeat_interval_ms`: BIGINT,
                    `repeat_spacing_ms`: BIGINT
                  >,
                  `time_based_escalation_config`: STRUCT<
                    `enabled`: INT,
                    `duration_ms`: BIGINT
                  >,
                  `interrupted_alert_replay_config`: STRUCT<
                    `enabled`: INT,
                    `played_fraction_threshold`: FLOAT
                  >
                >
              >,
              `silence_window_config`: STRUCT<
                `enabled`: INT,
                `silence_window_duration_ms`: BIGINT
              >,
              `reset_window_config`: STRUCT<
                `enabled`: INT,
                `reset_window_duration_ms`: BIGINT
              >,
              `lower_priority_pre_alert_delay_config`: STRUCT<
                `enabled`: INT,
                `duration_ms`: BIGINT
              >,
              `higher_priority_pre_alert_silence_config`: STRUCT<
                `enabled`: INT,
                `duration_ms`: BIGINT
              >,
              `alert_state_debounce_config`: STRUCT<
                `enabled`: INT,
                `duration_ms`: BIGINT
              >
            >
          >,
          `bundled_events_config`: STRUCT<
            `bundles`: ARRAY<
              STRUCT<
                `enabled`: INT,
                `bundle_name`: INT,
                `bundled_events`: ARRAY<INT>
              >
            >
          >
        >
      >,
      `cable_id_auto_sleep`: ARRAY<
        STRUCT<
          `cable_id`: INT,
          `sleep_config`: STRUCT<
            `sleep_delay_secs`: INT,
            `magnitude_threshold`: FLOAT,
            `sleep_duration_secs`: INT,
            `sleep_wake_duration_secs`: INT,
            `wake_on_battery`: BOOLEAN,
            `wake_on_power`: BOOLEAN,
            `sleep_override`: STRUCT<
              `override_timeout_secs`: INT,
              `sleep_duration_secs`: INT,
              `sleep_wake_duration_secs`: INT,
              `sleep_behavior`: INT,
              `use_persistent_override_timeout`: BOOLEAN,
              `persistent_override_timeout_secs`: BIGINT
            >,
            `wake_on_movement`: BOOLEAN,
            `wake_on_wifi_clients`: BOOLEAN,
            `sleep_behavior`: INT,
            `wake_while_no_gps`: BOOLEAN,
            `max_wake_secs`: INT,
            `accel_wake_config`: STRUCT<
              `high_pass_filter`: INT,
              `threshold`: INT,
              `duration`: INT
            >,
            `accel_enable_delay_secs`: BIGINT,
            `scheduled_wake_config`: STRUCT<
              `wake_at_times`: ARRAY<STRING>,
              `timezone`: STRING
            >,
            `enabled`: BOOLEAN,
            `wake_on_voltage_jump`: BOOLEAN,
            `voltage_jump_sleep_delay_secs`: BIGINT,
            `min_accel_interrupt_count`: BIGINT,
            `report_upload_sleep_delay_secs`: BIGINT,
            `obd_manager_active_in_moderate_power`: BOOLEAN,
            `disable_location_manager_in_moderate_power`: BOOLEAN,
            `disable_wifi_in_moderate_power`: BOOLEAN,
            `enable_widget_mgr_in_moderate_power`: BOOLEAN,
            `wake_on_panic_button`: BOOLEAN,
            `panic_button_wake_duration_ms`: BIGINT,
            `keep_wifi_bt_vreg_enabled_in_low_power`: BOOLEAN,
            `disable_force_ship_mode_power_config`: BOOLEAN,
            `required_work_sleep_delays`: ARRAY<
              STRUCT<
                `work_type`: INT,
                `max_delay_ms`: BIGINT
              >
            >,
            `hub_report_delay_after_required_work_ms`: BIGINT,
            `extend_full_power_reasons`: ARRAY<INT>,
            `reason_sleep_delay_ms_map`: ARRAY<
              STRUCT<
                `reason`: INT,
                `sleep_delay_ms`: DECIMAL(20, 0)
              >
            >,
            `enable_cellular_manager_in_low_power`: INT,
            `suspend_in_low_power`: INT,
            `enable_required_work_v2`: INT,
            `enable_usb_hub_power_off_delay`: INT,
            `usb_hub_power_off_delay_secs`: BIGINT,
            `keep_mcu_in_full_power_when_usb_hub_powered`: INT,
            `wake_on_movement_min_cable_millivolts`: BIGINT,
            `wake_on_can`: INT,
            `log_can_wake_event`: INT,
            `mdm9607_skip_full_power_between_moderate_to_low_power_transition_enabled`: INT,
            `boot_with_power_saving`: STRUCT<
              `enabled`: INT,
              `power_saving_reboot_reasons`: ARRAY<INT>,
              `normal_power_duration_secs`: BIGINT
            >,
            `wake_reason_stat_enabled`: INT,
            `wake_on_equipment_activity_enabled`: INT,
            `wake_on_voltage_jump_delay_enabled`: INT,
            `wake_on_voltage_jump_delay_ms`: BIGINT,
            `wake_on_movement_when_wake_on_can_enabled`: INT,
            `wake_on_aux_input_config`: STRUCT<
              `enabled`: INT,
              `sleep_delay_ms`: DECIMAL(20, 0),
              `aux_input_1_wake_config`: STRUCT<`wake_enabled`: INT>,
              `aux_input_2_wake_config`: STRUCT<`wake_enabled`: INT>,
              `aux_input_3_wake_config`: STRUCT<`wake_enabled`: INT>,
              `aux_input_4_wake_config`: STRUCT<`wake_enabled`: INT>,
              `aux_input_5_wake_config`: STRUCT<`wake_enabled`: INT>
            >,
            `brief_moderate_with_satellite_enabled`: INT,
            `brief_moderate_sleep_duration_secs`: BIGINT,
            `brief_moderate_sleep_wake_duration_secs`: BIGINT,
            `autodetect_towing_enabled`: INT,
            `wake_on_ble_sos_with_satellite_enabled`: INT,
            `wake_on_ble_sos_valid_duration_secs`: BIGINT,
            `wake_on_ble_sos_sleep_delay_duration_secs`: BIGINT
          >
        >
      >,
      `physical_security_config`: STRUCT<
        `door_relay_number`: BIGINT,
        `authorized_cards`: ARRAY<BIGINT>
      >,
      `cable_id_override`: STRUCT<`cable_id`: BIGINT>,
      `modbus_config`: STRUCT<
        `modbus_devices`: ARRAY<
          STRUCT<
            `modbus_type`: INT,
            `slave_id`: BIGINT,
            `serial_port_config`: STRUCT<
              `baud_rate`: INT,
              `data_bits`: INT,
              `stop_bits`: INT,
              `parity`: INT,
              `set_rs485_bias`: BOOLEAN,
              `rs485_termination_enabled`: BOOLEAN,
              `protocol`: INT,
              `port`: INT
            >,
            `hostname`: STRING,
            `port`: BIGINT,
            `modbus_pins`: ARRAY<
              STRUCT<
                `id`: BIGINT,
                `function_code`: INT,
                `starting_address`: BIGINT,
                `length`: BIGINT,
                `polling_interval_ms`: BIGINT,
                `scale_factor`: DOUBLE,
                `data_type`: INT,
                `word_swap`: BOOLEAN,
                `byte_swap`: BOOLEAN
              >
            >,
            `id`: BIGINT,
            `batch_contiguous_registers`: BOOLEAN,
            `use_ospmodbusreadv2`: BOOLEAN,
            `serial_port`: INT
          >
        >,
        `min_log_interval_ms`: BIGINT
      >,
      `ethernet_config`: STRUCT<
        `ethernet_mode`: INT,
        `static_ipv4_config`: STRUCT<
          `cidr_ip`: STRING,
          `default_gateway`: STRING,
          `route_subnet_traffic_only`: BOOLEAN
        >,
        `dns_config`: ARRAY<STRING>,
        `interface_num`: BIGINT,
        `router_config`: STRUCT<
          `dhcpd_config`: STRUCT<
            `static_leases`: ARRAY<
              STRUCT<
                `mac_address`: STRING,
                `ipv4`: STRING
              >
            >
          >
        >,
        `connectivity_check_interval_seconds`: INT
      >,
      `firebird_io_config`: STRUCT<
        `dio_configs`: ARRAY<
          STRUCT<
            `pin_number`: BIGINT,
            `dio_config`: INT,
            `module_id`: STRUCT<`id`: BIGINT>
          >
        >,
        `ai_configs`: ARRAY<
          STRUCT<
            `pin_number`: BIGINT,
            `ai_config`: INT,
            `module_id`: STRUCT<`id`: BIGINT>
          >
        >,
        `ao_configs`: ARRAY<
          STRUCT<
            `pin_number`: BIGINT,
            `ao_config`: INT,
            `value`: STRUCT<`value`: BIGINT>,
            `module_id`: STRUCT<`id`: BIGINT>
          >
        >,
        `plc_url`: STRING,
        `logging_period_ms`: BIGINT,
        `analog_threshold_mv`: BIGINT,
        `counter_configs`: ARRAY<
          STRUCT<
            `counter_filter_us`: INT,
            `module_id`: STRUCT<`id`: BIGINT>
          >
        >
      >,
      `tile_downloader_config`: STRUCT<
        `download_closest_num_tiles`: BIGINT,
        `max_bytes_to_store`: BIGINT
      >,
      `boot_recovery_mode`: INT,
      `battery_behavior`: STRUCT<
        `reboot_ms`: BIGINT,
        `go_to_moderate_power_ms`: BIGINT,
        `reboot_timing_debug_enabled`: INT
      >,
      `smartcard_servers`: STRUCT<
        `servers`: ARRAY<BIGINT>
      >,
      `device_id`: BIGINT,
      `multicam_config`: STRUCT<
        `nvr_default_ip_address`: STRING,
        `nvr_default_gateway`: STRING,
        `record_audio`: BOOLEAN,
        `multicam_camera_info`: ARRAY<
          STRUCT<
            `camera_id`: BIGINT,
            `camera_rotation`: INT,
            `stream_uuid`: STRING,
            `stream_id`: BIGINT,
            `flip_horizontal`: BOOLEAN,
            `flip_vertical`: BOOLEAN
          >
        >,
        `nvr_shut_off_delay_min`: BIGINT,
        `multicam_logger_config`: STRUCT<
          `log_level`: INT,
          `log_perf_metrics`: BOOLEAN
        >,
        `nvr_listener_config`: STRUCT<
          `enable`: BOOLEAN,
          `nvr_presence_timeout_mins`: BIGINT
        >,
        `report_nvr_logs`: BOOLEAN,
        `nvr_rtsp_proxy_config`: STRUCT<
          `enable`: BOOLEAN,
          `nvr_data_timeout_ms`: BIGINT,
          `nvr_request_timeout_ms`: BIGINT,
          `nvr_data_buffer_bytes_size`: BIGINT
        >,
        `nvr_client_timeouts`: STRUCT<
          `request_dial_timeout_ms`: BIGINT,
          `keep_alive_timeout_ms`: BIGINT,
          `response_header_timeout_ms`: BIGINT,
          `expect_continue_timeout_ms`: BIGINT
        >,
        `camera_monitor_config`: STRUCT<
          `default_stream_uuid`: STRING,
          `default_stream_id`: BIGINT,
          `monitor_layout`: INT,
          `monitor_stream_ids`: ARRAY<BIGINT>
        >,
        `still_capture_config`: STRUCT<
          `still_capture_interval_secs`: INT,
          `format`: INT,
          `jpeg_quality`: BIGINT,
          `jpeg_height_pixels`: BIGINT
        >,
        `valid_record_time_config`: ARRAY<
          STRUCT<
            `relative_start_ms`: BIGINT,
            `duration_ms`: BIGINT,
            `day_of_week`: INT
          >
        >,
        `stop_delay_ms`: DECIMAL(20, 0),
        `panic_button_report_length_secs`: BIGINT,
        `livestream_config`: STRUCT<
          `bitrate`: BIGINT,
          `framerate`: BIGINT,
          `codec`: INT,
          `resolution`: INT,
          `bitrate_control`: INT,
          `idr_interval_seconds`: INT,
          `video_transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >,
          `bframes_m_value`: INT
        >,
        `livestream_enabled`: INT,
        `high_res_encoded_stream_config`: STRUCT<
          `bitrate`: BIGINT,
          `framerate`: BIGINT,
          `codec`: INT,
          `resolution`: INT,
          `bitrate_control`: INT,
          `idr_interval_seconds`: INT,
          `video_transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >,
          `bframes_m_value`: INT
        >,
        `low_res_encoded_stream_config`: STRUCT<
          `bitrate`: BIGINT,
          `framerate`: BIGINT,
          `codec`: INT,
          `resolution`: INT,
          `bitrate_control`: INT,
          `idr_interval_seconds`: INT,
          `video_transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >,
          `bframes_m_value`: INT
        >,
        `high_res_storage_percent`: BIGINT,
        `low_res_recording_enabled`: INT,
        `camera_firmware_upgrade_enabled`: INT,
        `expect_octo_connected`: BOOLEAN,
        `expected_octos_connected`: BIGINT
      >,
      `programs_config`: STRUCT<
        `plc_programs`: ARRAY<
          STRUCT<
            `program_id`: BIGINT,
            `program_hash`: STRING,
            `program_uuid`: STRING
          >
        >
      >,
      `waze_beacon_mgr_config`: STRUCT<
        `window_length_ms`: BIGINT,
        `min_rssi_threshold`: INT,
        `beacon_locations_download_interval_ms`: BIGINT,
        `log_beacon_adv_samples`: BOOLEAN
      >,
      `local_hmi_config`: STRUCT<
        `local_hmis`: ARRAY<
          STRUCT<
            `id`: BIGINT,
            `name`: STRING,
            `hmi`: STRUCT<
              `components`: ARRAY<
                STRUCT<
                  `id`: BIGINT,
                  `name`: STRING,
                  `component_type`: INT,
                  `sources`: ARRAY<
                    STRUCT<
                      `machine_input`: STRUCT<`id`: BIGINT>,
                      `gateway_digital_output`: STRUCT<
                        `device_id`: BIGINT,
                        `pin_number`: BIGINT
                      >,
                      `modbus_output`: STRUCT<
                        `slave_device_id`: BIGINT,
                        `function_code`: BIGINT,
                        `address`: BIGINT,
                        `message`: STRUCT<`value`: BIGINT>
                      >,
                      `machine_output`: STRUCT<`uuid`: BINARY>,
                      `asset_report`: STRUCT<`uuid`: STRING>,
                      `data_group`: STRUCT<`data_group_id`: DECIMAL(20, 0)>,
                      `label`: STRING
                    >
                  >,
                  `position`: STRUCT<
                    `top`: BIGINT,
                    `left`: BIGINT,
                    `height`: BIGINT,
                    `width`: BIGINT,
                    `zIndex`: BIGINT
                  >,
                  `configuration`: STRUCT<
                    `single_value`: STRUCT<
                      `comparator`: INT,
                      `color_mappings`: ARRAY<
                        STRUCT<
                          `value`: BIGINT,
                          `operation`: INT,
                          `color`: STRING,
                          `double_value`: STRUCT<`value`: DOUBLE>
                        >
                      >,
                      `sparkline`: STRUCT<
                        `duration_ms`: BIGINT,
                        `increase_color`: STRING,
                        `decrease_color`: STRING
                      >,
                      `threshold_colors`: STRUCT<
                        `use_alert_threshold`: BOOLEAN,
                        `alert_id`: BIGINT,
                        `default_color`: STRUCT<`color`: INT>
                      >
                    >,
                    `progress_bar`: STRUCT<
                      `label_low`: STRING,
                      `label_high`: STRING,
                      `min`: BIGINT,
                      `max`: BIGINT,
                      `color_mappings`: ARRAY<
                        STRUCT<
                          `value`: BIGINT,
                          `operation`: INT,
                          `color`: STRING,
                          `double_value`: STRUCT<`value`: DOUBLE>
                        >
                      >
                    >,
                    `symbol`: STRUCT<`type`: INT>,
                    `text`: STRUCT<
                      `text`: STRING,
                      `color`: STRING
                    >,
                    `image`: STRUCT<`url`: STRING>,
                    `single_status_light`: STRUCT<
                      `color_mappings`: ARRAY<
                        STRUCT<
                          `value`: BIGINT,
                          `operation`: INT,
                          `color`: STRING,
                          `double_value`: STRUCT<`value`: DOUBLE>
                        >
                      >
                    >,
                    `double_status_light`: STRUCT<
                      `vertical`: BOOLEAN,
                      `first_light_color`: STRUCT<
                        `value`: BIGINT,
                        `operation`: INT,
                        `color`: STRING,
                        `double_value`: STRUCT<`value`: DOUBLE>
                      >,
                      `second_light_color`: STRUCT<
                        `value`: BIGINT,
                        `operation`: INT,
                        `color`: STRING,
                        `double_value`: STRUCT<`value`: DOUBLE>
                      >
                    >,
                    `toggle`: STRUCT<
                      `type`: INT,
                      `label_off`: STRING,
                      `label_on`: STRING
                    >,
                    `button`: STRUCT<
                      `type`: INT,
                      `label`: STRING,
                      `value`: BIGINT,
                      `color`: STRING,
                      `presets`: ARRAY<
                        STRUCT<
                          `label`: STRING,
                          `value`: STRING
                        >
                      >
                    >,
                    `mes_line`: STRUCT<`line_id`: BIGINT>,
                    `line_graph`: STRUCT<
                      `inputs`: ARRAY<
                        STRUCT<`id`: BIGINT>
                      >,
                      `second_axis_enabled`: BOOLEAN,
                      `secondary_inputs`: ARRAY<
                        STRUCT<`id`: BIGINT>
                      >,
                      `scale_axis_enabled`: BOOLEAN,
                      `axis_scale_type`: INT,
                      `data_groups`: STRUCT<
                        `data_group_ids`: ARRAY<DECIMAL(20, 0)>
                      >,
                      `secondary_data_groups`: STRUCT<
                        `data_group_ids`: ARRAY<DECIMAL(20, 0)>
                      >
                    >,
                    `number_display`: STRUCT<`num_decimals`: BIGINT>,
                    `bar_graph`: STRUCT<
                      `inputs`: ARRAY<
                        STRUCT<`id`: BIGINT>
                      >,
                      `bar_interval`: BIGINT,
                      `data_groups`: STRUCT<
                        `data_group_ids`: ARRAY<DECIMAL(20, 0)>
                      >
                    >,
                    `hmi_iframe`: STRUCT<
                      `device_id`: BIGINT,
                      `ip_address_and_port`: STRING
                    >,
                    `gauge`: STRUCT<
                      `warning_threshold`: DOUBLE,
                      `alarm_threshold`: DOUBLE,
                      `low_warning_threshold`: DOUBLE,
                      `low_alarm_threshold`: DOUBLE,
                      `threshold_colors`: STRUCT<
                        `use_alert_threshold`: BOOLEAN,
                        `alert_id`: BIGINT,
                        `default_color`: STRUCT<`color`: INT>
                      >,
                      `minimum_value`: DOUBLE
                    >,
                    `slider`: STRUCT<
                      `min`: DOUBLE,
                      `max`: DOUBLE
                    >,
                    `links`: STRUCT<
                      `dashboard_link`: STRUCT<
                        `dashboard_id`: BIGINT,
                        `dynamic_dashboard_uuid`: STRING
                      >,
                      `asset_link`: STRUCT<`asset_uuid`: STRING>
                    >,
                    `embedded_report`: STRUCT<
                      `report_type`: INT,
                      `show_only_asset_data`: BOOLEAN
                    >,
                    `control_form`: STRUCT<`individual_buttons`: BOOLEAN>,
                    `alarms`: STRUCT<`title`: STRING>
                  >,
                  `layout`: STRUCT<
                    `x`: BIGINT,
                    `y`: BIGINT,
                    `width`: BIGINT,
                    `height`: BIGINT
                  >
                >
              >,
              `dashboard_type`: INT,
              `heightPixels`: BIGINT,
              `widthPixels`: BIGINT,
              `default_time_range`: STRUCT<
                `end_ms`: DECIMAL(20, 0),
                `duration_ms`: DECIMAL(20, 0),
                `preset`: INT
              >
            >
          >
        >,
        `work_orders`: ARRAY<
          STRUCT<
            `org_id`: BIGINT,
            `id`: BIGINT,
            `name`: STRING,
            `recipe_id`: BIGINT,
            `quantity`: BIGINT,
            `complete`: BOOLEAN,
            `line`: STRUCT<`id`: BIGINT>,
            `date_info`: STRUCT<`production_date`: BIGINT>
          >
        >,
        `recipes`: ARRAY<
          STRUCT<
            `org_id`: BIGINT,
            `id`: BIGINT,
            `name`: STRING,
            `has_image`: BOOLEAN,
            `device_id`: BIGINT,
            `recipe_actions`: STRUCT<
              `start_actions`: ARRAY<
                STRUCT<
                  `machine_output_uuid`: BINARY,
                  `machine_output_value`: DOUBLE,
                  `machine_output_name`: STRING
                >
              >,
              `end_actions`: ARRAY<
                STRUCT<
                  `machine_output_uuid`: BINARY,
                  `machine_output_value`: DOUBLE,
                  `machine_output_name`: STRING
                >
              >
            >,
            `image_hash`: STRING,
            `product_config`: STRUCT<
              `target_count_rate`: BIGINT,
              `rate_period_ms`: BIGINT,
              `unit_config`: STRUCT<
                `units`: ARRAY<
                  STRUCT<
                    `scale_factor`: BIGINT,
                    `name`: STRING
                  >
                >,
                `default_unit`: STRING,
                `reporting_unit`: STRING
              >
            >,
            `machine_input_scale`: DOUBLE,
            `units`: STRING,
            `scalar_configs`: STRUCT<
              `configs`: ARRAY<
                STRUCT<
                  `machine_input_id`: BIGINT,
                  `scale`: DOUBLE,
                  `units`: STRING,
                  `line_config`: STRUCT<
                    `line_id`: BIGINT,
                    `line_name`: STRING,
                    `target_count_rate`: BIGINT,
                    `rate_period_ms`: BIGINT,
                    `data_inputs_scales`: ARRAY<
                      STRUCT<
                        `machine_input_id`: BIGINT,
                        `scale`: DOUBLE,
                        `units`: STRING
                      >
                    >
                  >
                >
              >
            >
          >
        >,
        `machine_inputs`: ARRAY<
          STRUCT<
            `id`: BIGINT,
            `name`: STRING,
            `units`: STRING
          >
        >,
        `lines`: ARRAY<
          STRUCT<
            `org_id`: BIGINT,
            `id`: BIGINT,
            `name`: STRING,
            `inputs`: ARRAY<
              STRUCT<
                `id`: BIGINT,
                `name`: STRING
              >
            >,
            `shift_schedule`: STRUCT<
              `shift_configs`: ARRAY<
                STRUCT<
                  `key`: STRING,
                  `relative_start_ms`: BIGINT,
                  `duration_ms`: BIGINT,
                  `day_of_week`: INT,
                  `downtime_allowances`: ARRAY<
                    STRUCT<
                      `reason`: STRING,
                      `duration_ms`: BIGINT,
                      `count`: BIGINT
                    >
                  >,
                  `scheduled_runs`: ARRAY<
                    STRUCT<
                      `product_id`: BIGINT,
                      `relative_start_ms`: BIGINT,
                      `duration_ms`: BIGINT,
                      `target_product_count`: BIGINT
                    >
                  >
                >
              >,
              `timezone`: STRING
            >,
            `form_config`: STRUCT<
              `sections`: ARRAY<
                STRUCT<
                  `label`: STRING,
                  `fields`: ARRAY<
                    STRUCT<
                      `label`: STRING,
                      `input_id`: BIGINT,
                      `data_type`: INT,
                      `enum_options`: ARRAY<
                        STRUCT<`name`: STRING>
                      >
                    >
                  >
                >
              >
            >,
            `input_assignments`: ARRAY<
              STRUCT<
                `assignment_type`: INT,
                `formula`: STRING,
                `input_sources`: ARRAY<
                  STRUCT<
                    `input_id`: BIGINT,
                    `variable`: STRING
                  >
                >,
                `interval_ms`: BIGINT,
                `counter_type`: INT
              >
            >,
            `minimum_downtime_ms`: BIGINT
          >
        >
      >,
      `anomaly_event_config`: STRUCT<
        `max_binarymessage_description_length`: BIGINT,
        `disable_binarymessage_logs`: BOOLEAN,
        `enable_s3binarymessage_logs`: BOOLEAN,
        `disable_anomaly_events`: BOOLEAN,
        `max_overall_message_length`: BIGINT
      >,
      `slaveserver_config`: STRUCT<
        `registers`: ARRAY<
          STRUCT<
            `id`: BIGINT,
            `data`: STRUCT<
              `type`: INT,
              `object_type`: INT,
              `object_stat`: INT,
              `object_id`: BIGINT,
              `pin_number`: BIGINT,
              `machine_input_id`: BIGINT,
              `machine_output_uuid`: STRING
            >,
            `connections`: ARRAY<
              STRUCT<
                `protocol`: INT,
                `rest_settings`: STRUCT<
                  `url`: STRING,
                  `key`: STRING
                >,
                `modbus_settings`: STRUCT<
                  `type`: INT,
                  `slave_addr`: BIGINT,
                  `code`: INT,
                  `register`: BIGINT,
                  `num_registers`: BIGINT,
                  `data_type`: INT,
                  `word_swap`: BOOLEAN,
                  `byte_swap`: BOOLEAN
                >,
                `mqtt_settings`: STRUCT<
                  `slave_device_id`: BIGINT,
                  `slave_pin_id`: BIGINT
                >,
                `data_output_settings`: STRUCT<
                  `machine_output_uuid`: STRING,
                  `interval_ms`: BIGINT
                >
              >
            >
          >
        >,
        `serial_cfg`: STRUCT<
          `baud_rate`: INT,
          `data_bits`: INT,
          `stop_bits`: INT,
          `parity`: INT,
          `set_rs485_bias`: BOOLEAN,
          `rs485_termination_enabled`: BOOLEAN,
          `protocol`: INT,
          `port`: INT
        >,
        `enabled_protocols`: ARRAY<INT>,
        `serial_port_protocol`: INT,
        `modbus_rtu_slave_id`: DECIMAL(20, 0),
        `serial_port`: INT
      >,
      `rolling_stop`: STRUCT<
        `v1`: STRUCT<
          `debounce_meters`: BIGINT,
          `window_begin_ms`: BIGINT,
          `window_end_ms`: BIGINT,
          `threshold_kmph`: BIGINT,
          `maximum_speed_kmph`: BIGINT
        >
      >,
      `vision`: STRUCT<
        `object_detection`: STRUCT<
          `use_dsp`: BOOLEAN,
          `confidence_threshold`: FLOAT,
          `debounce_meters`: BIGINT,
          `bbox_x_pad_pct`: FLOAT,
          `bbox_y_pad_pct`: FLOAT,
          `model_s3_uri`: STRING,
          `frame_rate`: INT,
          `model_version`: BIGINT,
          `detection_class_upload_flags`: DECIMAL(20, 0),
          `bbox_min_size_millipct`: BIGINT,
          `verbose_logging`: BOOLEAN,
          `metadata_logging_enabled`: BOOLEAN,
          `enable_cropping`: BOOLEAN,
          `crop_image_to_pct`: FLOAT,
          `max_fps`: BIGINT,
          `enable_lane_filtering`: INT
        >,
        `driver_monitoring`: STRUCT<
          `BufferLength`: BIGINT,
          `AvgConfidenceThresh`: FLOAT,
          `DistractionAlertThreshMs`: BIGINT,
          `StddevThresh`: FLOAT,
          `Roi`: STRUCT<
            `XLoMillipct`: BIGINT,
            `YLoMillipct`: BIGINT,
            `XHiMillipct`: BIGINT,
            `YHiMillipct`: BIGINT
          >,
          `EnableAudioAlerts`: BOOLEAN,
          `MinSpeedMk`: BIGINT,
          `MinStddev`: FLOAT,
          `MetadataLoggingEnabled`: BOOLEAN,
          `DistractionMsAlertThreshLow`: BIGINT,
          `DistractionMsAlertThreshMedium`: BIGINT,
          `DistractionMsAlertThreshHigh`: BIGINT,
          `EnableFeature`: BOOLEAN,
          `EnableDebugAudioAlerts`: BOOLEAN,
          `PoseEwmaLambda`: FLOAT,
          `DistractedAngleThreshMillideg`: BIGINT,
          `EnableLowSeverityEvents`: BOOLEAN,
          `HarshEventDebounceMs`: BIGINT,
          `EnableHeadposenetv2`: INT,
          `models`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `HacCropExpansionRatio`: FLOAT,
          `FaceDetectionThreshold`: FLOAT
        >,
        `time_to_collision`: STRUCT<
          `Tracking`: STRUCT<
            `IouThreshold`: FLOAT,
            `MissingFrameNumThreshold`: BIGINT,
            `ObjectEquivalenceIouThreshold`: FLOAT,
            `TriggeredFrameMsThreshold`: BIGINT,
            `TriggeredFrameNumThreshold`: BIGINT
          >,
          `LateralCollision`: STRUCT<
            `PathHistoryLen`: BIGINT,
            `VehicleCenterDiffPx`: INT
          >,
          `Alert`: STRUCT<
            `MinWidthPxThreshold`: BIGINT,
            `TtcSThreshold`: FLOAT,
            `MinSpeedKmph`: BIGINT,
            `AudioAlertPeriodMs`: BIGINT,
            `HarshEventDebounceMs`: BIGINT,
            `EnableAudioAlerts`: BOOLEAN,
            `TtcMsAlertThresholdLow`: BIGINT,
            `TtcMsAlertThresholdMedium`: BIGINT,
            `TtcMsAlertThresholdHigh`: BIGINT,
            `LowSpeedAlertParam`: STRUCT<
              `TtcSThreshold`: FLOAT,
              `MinSpeedKmph`: BIGINT
            >,
            `HighSpeedAlertParam`: STRUCT<
              `TtcSThreshold`: FLOAT,
              `MinSpeedKmph`: BIGINT
            >,
            `DisableAudioAtNight`: BOOLEAN
          >,
          `KalmanFilter`: STRUCT<
            `StateCovariance`: ARRAY<FLOAT>,
            `ProcessNoiseCovariance`: ARRAY<FLOAT>,
            `ObservationNoiseCovariance`: ARRAY<FLOAT>
          >,
          `EnableFeature`: BOOLEAN,
          `ConfidenceThreshold`: FLOAT,
          `MetadataLoggingEnabled`: BOOLEAN,
          `MinDxDyToPenalize`: FLOAT,
          `ValidTtcThresholdSeconds`: FLOAT,
          `CollisionPathRoiWidthPct`: FLOAT,
          `TrajectoryElementMaxAgeMs`: BIGINT,
          `MinTrajectoryHistoryMs`: INT,
          `MaxValidWidthToHeightRatio`: FLOAT,
          `PercentBottomFrameTreatedAsHood`: FLOAT,
          `DisableFeatureAtNight`: BOOLEAN
        >,
        `output_debug_frames`: BOOLEAN,
        `distance_estimation`: STRUCT<
          `CameraHeightMeters`: FLOAT,
          `HorizonLinePercent`: FLOAT
        >,
        `tailgating_estimation`: STRUCT<
          `EnableFeature`: BOOLEAN,
          `UnsafeFollowingDistanceSeconds`: FLOAT,
          `UnsafeFollowingDistanceAlertAfterSeconds`: FLOAT,
          `MinSpeedKmph`: BIGINT,
          `EnableAudioAlerts`: BOOLEAN,
          `AudioAlertDebounceMs`: BIGINT,
          `HarshEventDebounceMs`: BIGINT
        >,
        `turn_estimation`: STRUCT<
          `EnableFeature`: BOOLEAN,
          `SharpTurnWindowMs`: BIGINT,
          `SharpTurnAngleThresholdDegrees`: FLOAT
        >,
        `acceleration_estimation`: STRUCT<
          `EnableFeature`: BOOLEAN,
          `AccelerationEstimationWindowMs`: BIGINT,
          `AccelerationThresholdMs2`: FLOAT,
          `MinSpeedMarginKmph`: BIGINT
        >,
        `vanishingpoint_estimation`: STRUCT<
          `calibration_interval_seconds`: BIGINT,
          `calibration_duration_seconds`: BIGINT,
          `min_speed_kmph`: BIGINT,
          `max_turning_angle`: BIGINT,
          `use_detected_value`: BOOLEAN,
          `calibration_version`: BIGINT,
          `enabled`: INT
        >,
        `driver_album`: STRUCT<
          `enabled`: INT,
          `album_size_min`: INT,
          `album_size_max`: INT
        >,
        `lane_detection`: STRUCT<`enabled`: INT>,
        `driver_body_album`: STRUCT<
          `enabled`: INT,
          `album_size_min`: INT,
          `album_size_max`: INT
        >,
        `fastcv`: STRUCT<
          `primary_camera_use_dsp`: INT,
          `secondary_camera_use_dsp`: INT
        >,
        `secondary_inference`: STRUCT<
          `conditional_headpose_enabled`: INT,
          `inference_counters_enabled`: INT,
          `conditional_hand_face_detector_enabled`: INT,
          `test_mode`: STRUCT<
            `enabled`: INT,
            `num_hands_to_spoof`: BIGINT
          >
        >
      >,
      `eip_config`: STRUCT<
        `eip_devices`: ARRAY<
          STRUCT<
            `ip_address`: STRING,
            `cpu`: INT,
            `eip_tags`: ARRAY<
              STRUCT<
                `id`: BIGINT,
                `ioi_path`: STRING,
                `elem_type`: INT,
                `elem_count`: BIGINT,
                `name`: STRING,
                `polling_interval_ms`: BIGINT
              >
            >,
            `id`: BIGINT,
            `plc`: INT
          >
        >,
        `min_log_interval_ms`: BIGINT
      >,
      `stat_scaler_config`: STRUCT<
        `stat_mappings`: ARRAY<
          STRUCT<
            `machine_input_id`: BIGINT,
            `object_stat_key`: STRUCT<
              `object_type`: INT,
              `object_id`: BIGINT,
              `stat_type`: INT
            >,
            `formula`: STRING,
            `modbus_scale_factor`: DOUBLE,
            `modbus_data_type`: INT,
            `modbus_word_swap`: BOOLEAN,
            `modbus_byte_swap`: BOOLEAN,
            `is_remote`: BOOLEAN
          >
        >,
        `out_mappings`: ARRAY<
          STRUCT<
            `output_type`: INT,
            `slave_pin_config`: STRUCT<
              `id`: BIGINT,
              `modbus_settings`: STRUCT<
                `unchanged_bits_mask`: BIGINT,
                `use_value_as_bool_for_all_bits`: BOOLEAN
              >
            >,
            `local_variable_config`: STRUCT<`id`: BIGINT>,
            `gateway_channel_analog_config`: STRUCT<
              `id`: BIGINT,
              `mode`: INT,
              `module_id`: BIGINT
            >,
            `gateway_channel_digital_config`: STRUCT<
              `id`: BIGINT,
              `module_id`: BIGINT
            >,
            `uuid`: STRING,
            `rest_api_config`: STRUCT<
              `id`: BIGINT,
              `url`: STRING,
              `variable_key`: STRING,
              `device_id`: BIGINT
            >,
            `label_value_mappings`: ARRAY<
              STRUCT<
                `label`: STRING,
                `value`: DOUBLE
              >
            >,
            `widget_id`: BIGINT
          >
        >,
        `use_standard_units_for_analog_data_outputs`: BOOLEAN
      >,
      `tcp_forwarder_config`: STRUCT<
        `forwards`: ARRAY<
          STRUCT<
            `local_port`: BIGINT,
            `remote_gateway_id`: BIGINT,
            `remote_host`: STRING,
            `remote_port`: BIGINT
          >
        >
      >,
      `opc_config`: STRUCT<
        `opc_clients`: ARRAY<
          STRUCT<
            `auth_info`: STRUCT<
              `auth_method`: INT,
              `uri`: STRING
            >,
            `opc_nodes`: ARRAY<
              STRUCT<
                `id`: BIGINT,
                `namespace`: BIGINT,
                `name`: STRING,
                `node_name_type`: INT,
                `node_data_type`: INT
              >
            >
          >
        >,
        `min_log_interval_ms`: BIGINT
      >,
      `debug_logging`: STRUCT<
        `logcat`: STRUCT<
          `enable`: BOOLEAN,
          `interval_seconds`: BIGINT
        >,
        `tsens`: STRUCT<
          `enable`: BOOLEAN,
          `interval_seconds`: BIGINT,
          `metrics_enabled`: INT
        >,
        `system_stats`: STRUCT<
          `enable`: BOOLEAN,
          `interval_seconds`: BIGINT,
          `enable_verbose_memory_reporting`: BOOLEAN,
          `reboot_on_high_mem_used`: BOOLEAN,
          `reboot_mem_used_threshold_kb`: DECIMAL(20, 0),
          `enable_per_process_stats`: BOOLEAN,
          `log_high_cpu_usage_processes_enabled`: INT,
          `number_of_high_cpu_usage_processes_to_log`: BIGINT
        >,
        `power_stats`: STRUCT<
          `enable`: BOOLEAN,
          `interval_seconds`: BIGINT
        >,
        `services_metrics`: STRUCT<
          `enable`: BOOLEAN,
          `log_interval_seconds`: BIGINT
        >,
        `disk_stats_enabled`: INT,
        `pprof_profiling_enabled`: INT,
        `pprof_profiling_threshold_millipercent`: DECIMAL(20, 0)
      >,
      `mqtt_config`: STRUCT<
        `mqtt_brokers`: ARRAY<
          STRUCT<
            `id`: BIGINT,
            `uris`: ARRAY<STRING>,
            `auth_info`: STRUCT<
              `auth_type`: INT,
              `username`: STRING,
              `password`: STRING
            >,
            `topics`: ARRAY<
              STRUCT<
                `id`: BIGINT,
                `topic`: STRING
              >
            >
          >
        >
      >,
      `vehicle_simulator`: STRUCT<`enabled`: BOOLEAN>,
      `machine_vision_config`: STRUCT<
        `snapshot_config`: STRUCT<`snapshot_id`: DECIMAL(20, 0)>,
        `image_upload_rate_limiter_config`: STRUCT<
          `low_res_success_pixels_per_second`: DECIMAL(20, 0),
          `low_res_failure_pixels_per_second`: DECIMAL(20, 0),
          `high_res_success_pixels_per_second`: DECIMAL(20, 0),
          `high_res_failure_pixels_per_second`: DECIMAL(20, 0)
        >,
        `camera_limits_config`: STRUCT<
          `max_programs_count`: DECIMAL(20, 0),
          `max_steps_per_program_count`: DECIMAL(20, 0)
        >
      >,
      `turing_io_config`: STRUCT<
        `mod_config`: ARRAY<
          STRUCT<
            `slot_num`: BIGINT,
            `mod_type`: INT
          >
        >,
        `min_log_interval_ms`: BIGINT
      >,
      `voltage_jump_detector`: STRUCT<
        `polling_interval_ms`: BIGINT,
        `num_samples`: INT,
        `threshold_mv`: BIGINT,
        `min_mv`: BIGINT,
        `max_mv`: BIGINT,
        `enable_for_non_passenger_vehicles`: BOOLEAN,
        `use_min_voltage_in_window`: INT,
        `num_samples_config_enabled`: INT,
        `threshold_mv_config_enabled`: INT,
        `min_mv_config_enabled`: INT,
        `max_mv_config_enabled`: INT,
        `non_passenger_vehicles_enabled`: INT,
        `voltage_jump_detector_enabled`: INT
      >,
      `secondary_ethernet_config`: STRUCT<
        `ethernet_mode`: INT,
        `static_ipv4_config`: STRUCT<
          `cidr_ip`: STRING,
          `default_gateway`: STRING,
          `route_subnet_traffic_only`: BOOLEAN
        >,
        `dns_config`: ARRAY<STRING>,
        `interface_num`: BIGINT,
        `router_config`: STRUCT<
          `dhcpd_config`: STRUCT<
            `static_leases`: ARRAY<
              STRUCT<
                `mac_address`: STRING,
                `ipv4`: STRING
              >
            >
          >
        >,
        `connectivity_check_interval_seconds`: INT
      >,
      `traffic_side`: INT,
      `log_rotate_manager_config`: STRUCT<
        `system_log_space_bytes`: BIGINT,
        `misc_log_space_bytes`: BIGINT,
        `on_demand_delete_log_keep_bytes`: BIGINT
      >,
      `serial_port_protocol`: INT,
      `tacho_c_bitrates`: ARRAY<BIGINT>,
      `serial_slave_config`: STRUCT<
        `serial_devices`: ARRAY<
          STRUCT<
            `slave_device_id`: BIGINT,
            `port_number`: BIGINT,
            `config`: STRUCT<
              `baud_rate`: INT,
              `data_bits`: INT,
              `stop_bits`: INT,
              `parity`: INT,
              `set_rs485_bias`: BOOLEAN,
              `rs485_termination_enabled`: BOOLEAN,
              `protocol`: INT,
              `port`: INT
            >,
            `commands`: ARRAY<
              STRUCT<
                `slave_pin_id`: BIGINT,
                `command`: STRING,
                `response_regex`: STRING,
                `regex_match_index`: BIGINT,
                `regex_submatch_index`: BIGINT
              >
            >,
            `serial_port`: INT
          >
        >,
        `min_log_interval_ms`: BIGINT
      >,
      `engine_immobilizer_config`: STRUCT<
        `desired_state`: INT,
        `desired_state_changed_at_timestamp_utc_ms`: DECIMAL(20, 0),
        `desired_state_change_timeout_ms`: BIGINT,
        `safety_threshold_kmph`: DECIMAL(20, 0),
        `enable_safety_threshold_kmph`: INT,
        `use_obd_engine_state_enabled`: INT
      >,
      `roc_config`: STRUCT<
        `roc_devices`: ARRAY<
          STRUCT<
            `roc_type`: INT,
            `hostname`: STRING,
            `port`: BIGINT,
            `controller_unit`: BIGINT,
            `controller_group`: BIGINT,
            `host_unit`: BIGINT,
            `host_group`: BIGINT,
            `roc_tlps`: ARRAY<
              STRUCT<
                `id`: BIGINT,
                `tlp_data_type`: INT,
                `polling_interval_ms`: BIGINT,
                `type`: BIGINT,
                `logical`: BIGINT,
                `parameter`: BIGINT,
                `opcode_type`: INT
              >
            >,
            `serial_port_config`: STRUCT<
              `baud_rate`: INT,
              `data_bits`: INT,
              `stop_bits`: INT,
              `parity`: INT,
              `set_rs485_bias`: BOOLEAN,
              `rs485_termination_enabled`: BOOLEAN,
              `protocol`: INT,
              `port`: INT
            >,
            `interval_between_batch_request_ms`: BIGINT,
            `communication_timeout_ms`: DECIMAL(20, 0),
            `backoff_after_timeout_ms`: DECIMAL(20, 0),
            `backoff_after_crc_mismatch_ms`: DECIMAL(20, 0),
            `timezone`: STRING,
            `id`: BIGINT,
            `time_sync_enabled`: INT,
            `serial_port`: INT
          >
        >,
        `min_log_interval_ms`: BIGINT
      >,
      `watchdog_config`: STRUCT<
        `watchdog_enabled`: BOOLEAN,
        `hub_recovery`: STRUCT<
          `reboot_on_no_hub_enabled`: INT,
          `no_hub_reboot_mins`: BIGINT,
          `reboot_on_no_hub_when_inactive_enabled`: INT,
          `no_hub_when_inactive_reboot_mins`: BIGINT,
          `min_time_since_last_trip_inactive_mins`: BIGINT,
          `normal_power_is_activity_enabled`: INT
        >
      >,
      `tachograph_config`: STRUCT<
        `min_download_days`: BIGINT,
        `download_on_obd_wakeup`: BOOLEAN,
        `authenticate_when_needed_only`: BOOLEAN,
        `disable_downloads`: BOOLEAN,
        `allow_empty_card_number_authentication`: BOOLEAN,
        `enable_termination_resistor`: INT,
        `enable_limit_driver_downloads_to_daily`: INT,
        `locked_company_card_optimization_enabled`: INT,
        `tachograph_info_report_enabled`: INT,
        `tachograph_vu_file_speed_data_enabled`: INT,
        `alternative_isotp_vg_j1939_source_address`: BIGINT,
        `tachograph_info_report_enabled_v2`: INT,
        `vg34_increased_can_txqueuelen_enabled`: INT,
        `tachograph_vu_file_daily_download_enabled`: INT,
        `uds_iso16844_live_data_enabled`: INT,
        `tacho_serial_manager_enabled`: INT,
        `wake_on_d8_serial_enabled`: INT,
        `max_wake_on_d8_serial_between_trips`: BIGINT,
        `obd_engine_state_bus_message_enabled`: INT,
        `tachograph_driver_state_from_serial_object_stat_enabled`: INT,
        `tachograph_discovery_on_hgv_cables_can_a_enabled`: INT,
        `uds_iso16844_live_data_enabled_v2`: INT,
        `tacho_serial_manager_enabled_v2`: INT,
        `authentication_cache_enabled`: INT,
        `j1939_tco1_advanced_frame_check_enabled`: INT,
        `can_interface_manager_enabled`: INT,
        `filter_tco1_driving_state_enabled`: INT,
        `authentication_improvement_enabled`: INT,
        `local_driver_card_download_enabled`: INT
      >,
      `internalsyslogger_config`: STRUCT<`reboot_on_low_memory`: BOOLEAN>,
      `cm3x_reverse_proxy_config`: STRUCT<
        `enabled`: BOOLEAN,
        `externally_available`: BOOLEAN
      >,
      `cm3x_thermal_limiter_config`: STRUCT<
        `enabled`: BOOLEAN,
        `cpu_temp_threshold_c`: BIGINT,
        `check_interval_ms`: BIGINT,
        `sensors`: ARRAY<
          STRUCT<
            `name`: STRING,
            `entry_temp_c`: INT,
            `exit_temp_c`: INT
          >
        >,
        `auto_safe_mode_enabled`: BOOLEAN,
        `min_overheat_duration_ms`: BIGINT,
        `min_overheat_downgrade_duration_ms`: DECIMAL(20, 0),
        `enable_flip_mode`: BOOLEAN,
        `qcs603_thermal_limiter`: STRUCT<
          `ife_irq_affinity_enabled`: INT,
          `thermal_engine_enabled`: INT
        >
      >,
      `charging`: STRUCT<`ag24_solar_en_milliv`: BIGINT>,
      `serial_shell_config`: STRUCT<
        `port`: STRING,
        `enabled`: BOOLEAN
      >,
      `card_reader`: STRUCT<
        `omnikey_5027_read_timeout_ms`: BIGINT,
        `send_dashcam_report`: INT,
        `enable_reader_reset`: INT,
        `ignore_length_check_enabled`: INT,
        `permissive_mode_enabled`: INT,
        `goldbridge_reader`: STRUCT<
          `autoconfigure_enabled`: INT,
          `manual_config`: STRUCT<
            `enabled`: INT,
            `mode`: BIGINT,
            `leading_char`: BIGINT,
            `trailing_char`: BIGINT,
            `terminating_char`: BIGINT
          >
        >,
        `additional_readers`: ARRAY<
          STRUCT<
            `vendor_id`: BIGINT,
            `product_id`: BIGINT,
            `vendor_name`: STRING,
            `reader_config`: STRUCT<
              `read_timeout_ms`: BIGINT,
              `timeout_behavior`: INT,
              `card_len`: BIGINT,
              `max_buffer_size`: BIGINT,
              `parse_mode`: INT
            >
          >
        >,
        `send_failed_rfid_card_scans_enabled`: INT,
        `validate_rfid_card_scans_enabled`: INT
      >,
      `report_rcvd_cfg`: STRUCT<`enable`: BOOLEAN>,
      `grpc_hubserver_url`: STRING,
      `nvr_config`: STRUCT<
        `camera_devices`: ARRAY<
          STRUCT<
            `uuid`: STRING,
            `streams`: ARRAY<
              STRUCT<
                `uuid`: STRING,
                `rtsp_config`: STRUCT<
                  `uri`: STRING,
                  `username`: STRING,
                  `password`: STRING
                >,
                `serdes_config`: STRUCT<`device_port`: BIGINT>,
                `id`: BIGINT,
                `extra_channels`: ARRAY<
                  STRUCT<
                    `channel`: INT,
                    `rtsp_config`: STRUCT<
                      `uri`: STRING,
                      `username`: STRING,
                      `password`: STRING
                    >,
                    `store_to_disk`: INT,
                    `id`: BIGINT
                  >
                >,
                `default_channel_id`: BIGINT
              >
            >,
            `id`: BIGINT,
            `mac_address`: BIGINT,
            `enable_audio`: BOOLEAN
          >
        >,
        `storage_config`: STRUCT<`cleanup_threshold_percentage`: BIGINT>,
        `do_stream_with_initial_h264parse`: INT,
        `nvr_streamer_with_h264parse_embedded_enabled`: INT
      >,
      `wifi_config`: STRUCT<
        `enable_smartcard_wifi_client_mode`: BOOLEAN,
        `enable_wifi_on_asset_gateways`: INT,
        `reboot_on_interface_fail_enabled`: INT,
        `reboot_while_driving_enabled`: INT,
        `wpa3_support_enabled`: INT
      >,
      `voltage_processor_config`: STRUCT<
        `polling_interval_ms`: DECIMAL(20, 0),
        `voltage_jump_detector`: STRUCT<
          `polling_interval_ms`: BIGINT,
          `num_samples`: INT,
          `threshold_mv`: BIGINT,
          `min_mv`: BIGINT,
          `max_mv`: BIGINT,
          `enable_for_non_passenger_vehicles`: BOOLEAN,
          `use_min_voltage_in_window`: INT,
          `num_samples_config_enabled`: INT,
          `threshold_mv_config_enabled`: INT,
          `min_mv_config_enabled`: INT,
          `max_mv_config_enabled`: INT,
          `non_passenger_vehicles_enabled`: INT,
          `voltage_jump_detector_enabled`: INT
        >,
        `low_battery_detector_config`: STRUCT<
          `window_duration_ms`: DECIMAL(20, 0),
          `threshold_mv`: BIGINT,
          `rp1226_cable_threshold_mv`: BIGINT,
          `low_battery_detector_enabled`: INT
        >,
        `disable_max_voltage_jump_count_between_trips`: BOOLEAN,
        `max_voltage_jump_count_between_trips`: BIGINT,
        `polling_interval_ms_config_enabled`: INT,
        `max_voltage_jump_count_between_trips_enabled`: INT
      >,
      `vms_config`: STRUCT<
        `camera_devices`: ARRAY<
          STRUCT<
            `id`: BIGINT,
            `name`: STRING,
            `streams`: ARRAY<
              STRUCT<
                `id`: BIGINT,
                `object_detection_confidence_min_percent_override`: BIGINT,
                `generate_low_res_channel`: BOOLEAN,
                `uri`: STRING
              >
            >,
            `camera_type`: INT,
            `hostname`: STRING,
            `serial`: STRING,
            `username`: STRING,
            `password`: STRING,
            `mac_address`: BIGINT
          >
        >,
        `object_detection_confidence_min_percent_gateway_override`: BIGINT,
        `detection_analytics_config`: STRUCT<
          `enabled`: INT,
          `upload_interval_ms`: DECIMAL(20, 0),
          `min_instance_interval_ms`: DECIMAL(20, 0),
          `enable_diagnostics`: INT
        >,
        `ipcamera_mananger_config`: STRUCT<
          `network_scan_config`: STRUCT<
            `enabled`: INT,
            `scan_local_network`: INT,
            `additional_netblocks`: ARRAY<STRING>,
            `scan_interval_seconds`: BIGINT,
            `scan_timeout_seconds`: BIGINT
          >
        >,
        `local_hls_streaming_enabled`: INT
      >,
      `s7_config`: STRUCT<
        `s7_devices`: ARRAY<
          STRUCT<
            `hostname`: STRING,
            `rack`: DECIMAL(20, 0),
            `slot`: DECIMAL(20, 0),
            `s7_tags`: ARRAY<
              STRUCT<
                `id`: DECIMAL(20, 0),
                `operand`: INT,
                `datatype`: INT,
                `address`: DECIMAL(20, 0),
                `bit`: DECIMAL(20, 0),
                `length`: DECIMAL(20, 0),
                `database_number`: DECIMAL(20, 0)
              >
            >,
            `slave_id`: DECIMAL(20, 0)
          >
        >,
        `min_log_interval_ms`: DECIMAL(20, 0)
      >,
      `vulcan_io_config`: STRUCT<
        `module_config`: ARRAY<
          STRUCT<
            `module_id`: DECIMAL(20, 0),
            `module_type`: INT,
            `ai_config`: STRUCT<
              `ai_chan_setting_pairs`: ARRAY<
                STRUCT<
                  `channel`: BIGINT,
                  `setting`: INT
                >
              >
            >,
            `ao_config`: STRUCT<
              `ao_chan_setting_pairs`: ARRAY<
                STRUCT<
                  `channel`: BIGINT,
                  `setting`: INT
                >
              >
            >,
            `minimum_log_interval`: DECIMAL(20, 0),
            `dio_config`: STRUCT<
              `dio_bank_setting_pairs`: ARRAY<
                STRUCT<
                  `bank`: BIGINT,
                  `setting`: INT
                >
              >
            >,
            `sys_config`: STRUCT<
              `serial_config`: STRUCT<
                `mux_control`: INT,
                `serial_mode`: INT,
                `rs485_bias_enabled`: BOOLEAN,
                `rs485_termination_enabled`: BOOLEAN
              >,
              `can_config`: STRUCT<
                `mux_control`: INT,
                `can_mode`: INT,
                `termination_enabled`: BOOLEAN
              >
            >
          >
        >,
        `config_version`: DECIMAL(20, 0),
        `disable_all_mcu_fwup`: BOOLEAN,
        `disable_io_module_mcu_fwup`: BOOLEAN
      >,
      `cellular_accounting_config`: STRUCT<`disabled`: BOOLEAN>,
      `tile_manager_config`: STRUCT<
        `enabled`: BOOLEAN,
        `base_url`: STRING,
        `max_tile_count`: BIGINT,
        `max_tile_bytes_to_store`: BIGINT,
        `zoom_level`: BIGINT,
        `stop_sign_overrides`: ARRAY<
          STRUCT<
            `stop_location_type`: INT,
            `node_id`: DECIMAL(20, 0),
            `way_ids`: ARRAY<DECIMAL(20, 0)>,
            `lat`: DOUBLE,
            `lng`: DOUBLE,
            `valid_bearings`: BIGINT
          >
        >,
        `secure_base_url`: STRING,
        `enable_secure_fetches`: INT,
        `secure_cookie_fetch_path`: STRING,
        `tile_check_period_ms`: DECIMAL(20, 0),
        `tiles_cache_key`: STRING,
        `override_check_period_ms`: DECIMAL(20, 0),
        `overrides_cache_key`: STRING,
        `enable_s3_file_tile_overrides`: INT,
        `s3_file_tile_overrides_base_url`: STRING,
        `tile_version`: STRING,
        `bridge_location_enabled`: INT,
        `bridge_location_base_url`: STRING,
        `bridge_location_version`: STRING,
        `bridge_location_cache_key`: STRING,
        `decoupled_rolling_stops_hack_enabled`: INT,
        `layered_tiles_enabled`: INT,
        `tile_layer_configs`: ARRAY<
          STRUCT<
            `layer_name`: INT,
            `cache_file_extension`: STRING,
            `server_file_extension`: STRING,
            `cache_key`: STRING,
            `version`: STRING,
            `base_url`: STRING
          >
        >,
        `tile_layer_config_overrides`: ARRAY<
          STRUCT<
            `layer_name`: INT,
            `cache_file_extension`: STRING,
            `server_file_extension`: STRING,
            `cache_key`: STRING,
            `version`: STRING,
            `base_url`: STRING
          >
        >,
        `tile_caching_on_banshee_storage_enabled`: INT
      >,
      `speed_limit_alert`: STRUCT<
        `enabled`: BOOLEAN,
        `min_speed_kmph`: FLOAT,
        `kmph_over_limit_enabled`: BOOLEAN,
        `kmph_over_limit_threshold`: FLOAT,
        `percent_over_limit_enabled`: BOOLEAN,
        `percent_over_limit_threshold`: FLOAT,
        `enable_audio_alerts`: BOOLEAN,
        `audio_alert_debounce_ms`: BIGINT,
        `harsh_event_debounce_ms`: BIGINT,
        `min_duration_before_alert_ms`: BIGINT,
        `vehicle_types`: ARRAY<BIGINT>,
        `enable_metadata_logging`: INT,
        `speed_limit_log_interval_secs`: BIGINT,
        `harsh_events_enabled`: INT,
        `max_consecutive_audio_alerts`: BIGINT,
        `speed_limit_log_on_every_way_enabled`: INT,
        `read_speed_limit_from_base_tile_enabled`: INT,
        `tile_layers`: ARRAY<INT>,
        `tile_layer_overrides`: ARRAY<INT>,
        `audio_alert_policy_manager_enabled`: INT
      >,
      `distracted_policy_detector_config`: STRUCT<
        `phone_policy_config`: STRUCT<
          `enabled`: BOOLEAN,
          `enabled_while_stationary`: BOOLEAN,
          `min_speed_kmph`: BIGINT,
          `left_side_enabled`: BOOLEAN,
          `right_side_enabled`: BOOLEAN,
          `midpoint_normalized`: FLOAT,
          `enable_audio_alerts`: BOOLEAN,
          `audio_alert_debounce_ms`: BIGINT,
          `harsh_event_debounce_ms`: BIGINT,
          `confidence_threshold`: FLOAT,
          `enable_object_stat`: INT,
          `enable_object_stat_image_upload`: INT,
          `enable_harsh_events`: INT,
          `enable_trip_start_alerts_delay`: INT,
          `trip_start_alerts_delay_ms`: BIGINT,
          `enable_during_trip`: INT,
          `detection_window`: STRUCT<
            `frames_in_window`: BIGINT,
            `frames_in_violation_threshold`: BIGINT
          >,
          `audio_alerts_at_night_enabled`: BOOLEAN
        >,
        `smoking_policy_config`: STRUCT<
          `enabled`: BOOLEAN,
          `enabled_while_stationary`: BOOLEAN,
          `min_speed_kmph`: BIGINT,
          `left_side_enabled`: BOOLEAN,
          `right_side_enabled`: BOOLEAN,
          `midpoint_normalized`: FLOAT,
          `enable_audio_alerts`: BOOLEAN,
          `audio_alert_debounce_ms`: BIGINT,
          `harsh_event_debounce_ms`: BIGINT,
          `confidence_threshold`: FLOAT,
          `enable_object_stat`: INT,
          `enable_object_stat_image_upload`: INT,
          `enable_harsh_events`: INT,
          `enable_trip_start_alerts_delay`: INT,
          `trip_start_alerts_delay_ms`: BIGINT,
          `enable_during_trip`: INT,
          `detection_window`: STRUCT<
            `frames_in_window`: BIGINT,
            `frames_in_violation_threshold`: BIGINT
          >,
          `audio_alerts_at_night_enabled`: BOOLEAN
        >,
        `food_policy_config`: STRUCT<
          `enabled`: BOOLEAN,
          `enabled_while_stationary`: BOOLEAN,
          `min_speed_kmph`: BIGINT,
          `left_side_enabled`: BOOLEAN,
          `right_side_enabled`: BOOLEAN,
          `midpoint_normalized`: FLOAT,
          `enable_audio_alerts`: BOOLEAN,
          `audio_alert_debounce_ms`: BIGINT,
          `harsh_event_debounce_ms`: BIGINT,
          `confidence_threshold`: FLOAT,
          `enable_object_stat`: INT,
          `enable_object_stat_image_upload`: INT,
          `enable_harsh_events`: INT,
          `enable_trip_start_alerts_delay`: INT,
          `trip_start_alerts_delay_ms`: BIGINT,
          `enable_during_trip`: INT,
          `detection_window`: STRUCT<
            `frames_in_window`: BIGINT,
            `frames_in_violation_threshold`: BIGINT
          >,
          `audio_alerts_at_night_enabled`: BOOLEAN
        >,
        `drink_policy_config`: STRUCT<
          `enabled`: BOOLEAN,
          `enabled_while_stationary`: BOOLEAN,
          `min_speed_kmph`: BIGINT,
          `left_side_enabled`: BOOLEAN,
          `right_side_enabled`: BOOLEAN,
          `midpoint_normalized`: FLOAT,
          `enable_audio_alerts`: BOOLEAN,
          `audio_alert_debounce_ms`: BIGINT,
          `harsh_event_debounce_ms`: BIGINT,
          `confidence_threshold`: FLOAT,
          `enable_object_stat`: INT,
          `enable_object_stat_image_upload`: INT,
          `enable_harsh_events`: INT,
          `enable_trip_start_alerts_delay`: INT,
          `trip_start_alerts_delay_ms`: BIGINT,
          `enable_during_trip`: INT,
          `detection_window`: STRUCT<
            `frames_in_window`: BIGINT,
            `frames_in_violation_threshold`: BIGINT
          >,
          `audio_alerts_at_night_enabled`: BOOLEAN
        >,
        `seatbelt_config`: STRUCT<
          `enabled`: BOOLEAN,
          `enabled_while_stationary`: BOOLEAN,
          `min_speed_kmph`: BIGINT,
          `left_side_enabled`: BOOLEAN,
          `right_side_enabled`: BOOLEAN,
          `midpoint_normalized`: FLOAT,
          `enable_audio_alerts`: BOOLEAN,
          `audio_alert_debounce_ms`: BIGINT,
          `harsh_event_debounce_ms`: BIGINT,
          `confidence_threshold`: FLOAT,
          `enable_object_stat`: INT,
          `enable_object_stat_image_upload`: INT,
          `enable_harsh_events`: INT,
          `enable_trip_start_alerts_delay`: INT,
          `trip_start_alerts_delay_ms`: BIGINT,
          `enable_during_trip`: INT,
          `detection_window`: STRUCT<
            `frames_in_window`: BIGINT,
            `frames_in_violation_threshold`: BIGINT
          >,
          `audio_alerts_at_night_enabled`: BOOLEAN
        >,
        `mask_config`: STRUCT<
          `enabled`: BOOLEAN,
          `enabled_while_stationary`: BOOLEAN,
          `min_speed_kmph`: BIGINT,
          `left_side_enabled`: BOOLEAN,
          `right_side_enabled`: BOOLEAN,
          `midpoint_normalized`: FLOAT,
          `enable_audio_alerts`: BOOLEAN,
          `audio_alert_debounce_ms`: BIGINT,
          `harsh_event_debounce_ms`: BIGINT,
          `confidence_threshold`: FLOAT,
          `enable_object_stat`: INT,
          `enable_object_stat_image_upload`: INT,
          `enable_harsh_events`: INT,
          `enable_trip_start_alerts_delay`: INT,
          `trip_start_alerts_delay_ms`: BIGINT,
          `enable_during_trip`: INT,
          `detection_window`: STRUCT<
            `frames_in_window`: BIGINT,
            `frames_in_violation_threshold`: BIGINT
          >,
          `audio_alerts_at_night_enabled`: BOOLEAN
        >,
        `driver_obstruction_config`: STRUCT<
          `enabled`: BOOLEAN,
          `enabled_while_stationary`: BOOLEAN,
          `min_speed_kmph`: BIGINT,
          `left_side_enabled`: BOOLEAN,
          `right_side_enabled`: BOOLEAN,
          `midpoint_normalized`: FLOAT,
          `enable_audio_alerts`: BOOLEAN,
          `audio_alert_debounce_ms`: BIGINT,
          `harsh_event_debounce_ms`: BIGINT,
          `confidence_threshold`: FLOAT,
          `enable_object_stat`: INT,
          `enable_object_stat_image_upload`: INT,
          `enable_harsh_events`: INT,
          `enable_trip_start_alerts_delay`: INT,
          `trip_start_alerts_delay_ms`: BIGINT,
          `enable_during_trip`: INT,
          `detection_window`: STRUCT<
            `frames_in_window`: BIGINT,
            `frames_in_violation_threshold`: BIGINT
          >,
          `audio_alerts_at_night_enabled`: BOOLEAN
        >,
        `passenger_config`: STRUCT<
          `enabled`: BOOLEAN,
          `enabled_while_stationary`: BOOLEAN,
          `min_speed_kmph`: BIGINT,
          `left_side_enabled`: BOOLEAN,
          `right_side_enabled`: BOOLEAN,
          `midpoint_normalized`: FLOAT,
          `enable_audio_alerts`: BOOLEAN,
          `audio_alert_debounce_ms`: BIGINT,
          `harsh_event_debounce_ms`: BIGINT,
          `confidence_threshold`: FLOAT,
          `enable_object_stat`: INT,
          `enable_object_stat_image_upload`: INT,
          `enable_harsh_events`: INT,
          `enable_trip_start_alerts_delay`: INT,
          `trip_start_alerts_delay_ms`: BIGINT,
          `enable_during_trip`: INT,
          `detection_window`: STRUCT<
            `frames_in_window`: BIGINT,
            `frames_in_violation_threshold`: BIGINT
          >,
          `audio_alerts_at_night_enabled`: BOOLEAN
        >,
        `outward_obstruction_config`: STRUCT<
          `enabled`: BOOLEAN,
          `enabled_while_stationary`: BOOLEAN,
          `min_speed_kmph`: BIGINT,
          `left_side_enabled`: BOOLEAN,
          `right_side_enabled`: BOOLEAN,
          `midpoint_normalized`: FLOAT,
          `enable_audio_alerts`: BOOLEAN,
          `audio_alert_debounce_ms`: BIGINT,
          `harsh_event_debounce_ms`: BIGINT,
          `confidence_threshold`: FLOAT,
          `enable_object_stat`: INT,
          `enable_object_stat_image_upload`: INT,
          `enable_harsh_events`: INT,
          `enable_trip_start_alerts_delay`: INT,
          `trip_start_alerts_delay_ms`: BIGINT,
          `enable_during_trip`: INT,
          `detection_window`: STRUCT<
            `frames_in_window`: BIGINT,
            `frames_in_violation_threshold`: BIGINT
          >,
          `audio_alerts_at_night_enabled`: BOOLEAN
        >,
        `models`: STRUCT<
          `dlcs`: ARRAY<
            STRUCT<
              `name`: STRING,
              `version`: STRING,
              `url`: STRING,
              `size`: BIGINT,
              `sha256`: BINARY,
              `preferred_runtime`: INT
            >
          >,
          `model_registry_key`: STRING
        >
      >,
      `video_processing_config`: STRUCT<
        `camera_devices`: ARRAY<
          STRUCT<
            `id`: BIGINT,
            `streams`: ARRAY<
              STRUCT<
                `id`: BIGINT,
                `ingress_channel`: BIGINT,
                `encoder_configs`: ARRAY<
                  STRUCT<
                    `codec`: INT,
                    `keyframe_interval_frames`: BIGINT,
                    `rate_control_mode`: INT,
                    `set_rate_control_mode`: INT,
                    `bitrate_bits_per_second`: BIGINT,
                    `hardware_preset`: INT,
                    `set_hardware_preset`: INT,
                    `h264_profile`: INT,
                    `set_h264_profile`: INT,
                    `h264_entropy_codec`: INT,
                    `set_h264_entropy_codec`: INT,
                    `width`: BIGINT,
                    `height`: BIGINT,
                    `framerate_numerator`: BIGINT,
                    `framerate_denominator`: BIGINT,
                    `egress_channel`: BIGINT
                  >
                >
              >
            >
          >
        >,
        `primary_object_detector_model_version`: STRING,
        `use_deepstream_v2`: INT
      >,
      `tile_rolling_stop`: STRUCT<
        `stop_sign_policy`: STRUCT<
          `enabled`: BOOLEAN,
          `stop_threshold_kmph`: FLOAT,
          `enable_audio_alerts`: BOOLEAN,
          `audio_alert_debounce_ms`: BIGINT,
          `harsh_event_debounce_ms`: BIGINT,
          `distance_thresholds`: STRUCT<
            `stop_zone_enter_radius_meters`: FLOAT,
            `stop_zone_exit_radius_meters`: FLOAT,
            `speed_lookback_distance_meters`: FLOAT
          >,
          `ignore_stop_config`: STRUCT<
            `ignore_stop_enabled`: INT,
            `ignore_stop_threshold_kmph`: FLOAT,
            `harsh_event_upload_enabled`: INT
          >,
          `gate_audio_alert_on_cv_detection_policy`: STRUCT<
            `enabled`: INT,
            `detection_max_age_ms`: BIGINT,
            `detection_min_age_ms`: BIGINT,
            `use_any_cv_detection_in_lookback_window_enabled`: INT,
            `lookback_window_relative_to_harsh_event_timestamp_enabled`: INT,
            `gate_action_if_cv_detection_after_lookback_window_enabled`: INT
          >,
          `gate_harsh_event_on_cv_detection_policy`: STRUCT<
            `enabled`: INT,
            `detection_max_age_ms`: BIGINT,
            `detection_min_age_ms`: BIGINT,
            `use_any_cv_detection_in_lookback_window_enabled`: INT,
            `lookback_window_relative_to_harsh_event_timestamp_enabled`: INT,
            `gate_action_if_cv_detection_after_lookback_window_enabled`: INT
          >
        >,
        `railroad_crossing_policy`: STRUCT<
          `enabled`: BOOLEAN,
          `stop_threshold_kmph`: FLOAT,
          `enable_audio_alerts`: BOOLEAN,
          `audio_alert_debounce_ms`: BIGINT,
          `harsh_event_debounce_ms`: BIGINT,
          `distance_thresholds`: STRUCT<
            `stop_zone_enter_radius_meters`: FLOAT,
            `stop_zone_exit_radius_meters`: FLOAT,
            `speed_lookback_distance_meters`: FLOAT
          >,
          `ignore_stop_config`: STRUCT<
            `ignore_stop_enabled`: INT,
            `ignore_stop_threshold_kmph`: FLOAT,
            `harsh_event_upload_enabled`: INT
          >,
          `shadow_event_logging_enabled`: INT
        >,
        `enable_metadata_logging`: INT,
        `rolling_stop_internal_params`: STRUCT<
          `process_period_ms`: BIGINT,
          `stop_search_radius_meters`: FLOAT,
          `override_min_distance_between_stops_meters`: INT,
          `min_distance_between_stops_meters`: FLOAT,
          `override_crossing_valid_within_meters`: INT,
          `crossing_valid_within_meters`: FLOAT,
          `processed_stop_harsh_event_upload_enabled`: INT,
          `stop_location_crossing_shadow_logging_enabled`: INT,
          `use_processed_stop_locations_map_enabled`: INT,
          `harsh_event_filter_reason_logging_enabled`: INT,
          `most_upstream_reason_to_log`: INT,
          `use_processed_stop_crossings_map_enabled`: INT,
          `min_distance_to_allow_detecting_same_crossing_meters`: BIGINT,
          `allowed_bearing_deviation_degrees_clockwise`: BIGINT,
          `allowed_bearing_deviation_degrees_counter_clockwise`: BIGINT
        >,
        `read_stop_locations_from_base_tile_enabled`: INT,
        `tile_layers`: ARRAY<INT>,
        `tile_layer_overrides`: ARRAY<INT>,
        `traffic_light_policy`: STRUCT<
          `enabled`: INT,
          `audio_alerts_enabled`: INT,
          `shadow_event_logging_enabled`: INT,
          `audio_alert_debounce_ms`: BIGINT,
          `harsh_event_debounce_ms`: BIGINT,
          `distance_thresholds`: STRUCT<
            `stop_zone_enter_radius_meters`: FLOAT,
            `stop_zone_exit_radius_meters`: FLOAT,
            `speed_lookback_distance_meters`: FLOAT
          >,
          `detection_policy`: STRUCT<
            `detection_max_age_ms`: BIGINT,
            `detection_min_age_ms`: BIGINT,
            `detection_history_age_ms`: BIGINT,
            `min_red_light_percent`: FLOAT
          >
        >
      >,
      `local_variables_config`: STRUCT<
        `variables`: ARRAY<
          STRUCT<`local_var_id`: BIGINT>
        >
      >,
      `network_discovery_service`: STRUCT<`disabled`: BOOLEAN>,
      `persistent_io_config`: STRUCT<`enabled`: BOOLEAN>,
      `services_manager_config`: STRUCT<
        `disabled_services`: ARRAY<STRING>,
        `disable_reboot_on_unkillable_service`: BOOLEAN,
        `max_service_kill_attempts_before_reboot`: BIGINT,
        `run_golang_hub_client_always_deprecated`: INT,
        `run_golang_hub_client_on_odd_boot_count_deprecated`: INT,
        `run_golang_hub_client_always`: INT,
        `run_golang_hub_client_on_odd_boot_count`: INT,
        `process_merge_config`: STRUCT<
          `enable_lowpower_process_merge`: INT,
          `enable_hwwatchdog_process_merge`: INT,
          `enable_publicwebserver_process_merge`: INT,
          `enable_hub_client_process_merge`: INT
        >,
        `run_relay_manager_in_mdm_nonstop_enabled`: INT,
        `run_publicwebserver_as_not_root_enabled`: INT,
        `vg_safety_managers_config`: STRUCT<
          `run_driver_sign_in_alerts_manager_in_camera_service_enabled`: INT,
          `run_tile_update_manager_in_camera_service_enabled`: INT,
          `run_map_match_manager_in_camera_service_enabled`: INT,
          `run_alerts_manager_in_camera_service_enabled`: INT,
          `run_full_audio_manager_in_camera_service_enabled`: INT,
          `run_banshee_storage_manager_in_camera_service_enabled`: INT,
          `run_low_bridge_strike_manager_in_camera_service_enabled`: INT
        >,
        `run_time_manager_in_network_manager_enabled`: INT
      >,
      `deprecated_disable_ship_mode`: BOOLEAN,
      `stop_location_overrides`: STRUCT<
        `stop_locations`: ARRAY<
          STRUCT<
            `stop_location`: STRUCT<
              `stop_location_type`: INT,
              `node_id`: DECIMAL(20, 0),
              `way_ids`: ARRAY<DECIMAL(20, 0)>,
              `lat`: DOUBLE,
              `lng`: DOUBLE,
              `valid_bearings`: BIGINT
            >,
            `location_removed`: BOOLEAN
          >
        >
      >,
      `livestream_webrtc_server_config`: STRUCT<
        `stun_servers`: ARRAY<STRING>,
        `dashcam_config`: STRUCT<
          `primary`: STRUCT<
            `bitrate`: BIGINT,
            `framerate`: BIGINT,
            `codec`: INT,
            `resolution`: INT,
            `bitrate_control`: INT,
            `idr_interval_seconds`: INT,
            `video_transform`: STRUCT<
              `flip_vertical`: BOOLEAN,
              `flip_horizontal`: BOOLEAN,
              `rotation`: INT
            >,
            `bframes_m_value`: INT
          >,
          `secondary`: STRUCT<
            `bitrate`: BIGINT,
            `framerate`: BIGINT,
            `codec`: INT,
            `resolution`: INT,
            `bitrate_control`: INT,
            `idr_interval_seconds`: INT,
            `video_transform`: STRUCT<
              `flip_vertical`: BOOLEAN,
              `flip_horizontal`: BOOLEAN,
              `rotation`: INT
            >,
            `bframes_m_value`: INT
          >,
          `primary_thumbnail`: STRUCT<
            `bitrate`: BIGINT,
            `framerate`: BIGINT,
            `codec`: INT,
            `resolution`: INT,
            `bitrate_control`: INT,
            `idr_interval_seconds`: INT,
            `video_transform`: STRUCT<
              `flip_vertical`: BOOLEAN,
              `flip_horizontal`: BOOLEAN,
              `rotation`: INT
            >,
            `bframes_m_value`: INT
          >,
          `primary_thumbnail_scale`: FLOAT,
          `secondary_thumbnail`: STRUCT<
            `bitrate`: BIGINT,
            `framerate`: BIGINT,
            `codec`: INT,
            `resolution`: INT,
            `bitrate_control`: INT,
            `idr_interval_seconds`: INT,
            `video_transform`: STRUCT<
              `flip_vertical`: BOOLEAN,
              `flip_horizontal`: BOOLEAN,
              `rotation`: INT
            >,
            `bframes_m_value`: INT
          >,
          `secondary_thumbnail_scale`: FLOAT
        >,
        `max_session_duration_secs`: BIGINT,
        `max_concurrent_peers`: BIGINT,
        `connection_timeout_secs`: BIGINT,
        `opus_encoder_config`: STRUCT<
          `complexity`: BIGINT,
          `packet_loss_percent`: BIGINT
        >,
        `monthly_quota_secs`: INT,
        `udp_mux_enabled`: INT,
        `restrict_to_hubclient_interface`: BOOLEAN
      >,
      `vulcan_mfg_test_runner_config`: STRUCT<
        `tests`: ARRAY<
          STRUCT<
            `type`: INT,
            `custom_command`: STRING,
            `time_between_iterations_ms`: DECIMAL(20, 0)
          >
        >,
        `load_config`: STRUCT<
          `cpu_load`: STRUCT<`target_one_minute_millipercent`: DECIMAL(20, 0)>,
          `memory_load`: STRUCT<`target_millipercent`: DECIMAL(20, 0)>,
          `network_load`: STRUCT<
            `interface_targets`: ARRAY<
              STRUCT<
                `network_interface`: INT,
                `target_kilobytes_per_second`: DECIMAL(20, 0),
                `cellular_authorization_end_ms`: DECIMAL(20, 0)
              >
            >,
            `address`: STRING
          >,
          `usb_load`: STRUCT<`target_kilobytes_per_second`: DECIMAL(20, 0)>
        >
      >,
      `public_product_name`: STRING,
      `vg_mcu_manager_config`: STRUCT<
        `spi_watchdog_timeout_ms`: BIGINT,
        `disable_mcu_upgrades`: BOOLEAN,
        `cable_id_string_override`: STRING,
        `modi_use_alternate_digital_input_for_spi`: BOOLEAN,
        `enable_panic_and_reboot_when_spi_hangs_on_startup`: INT,
        `can_standby_in_low_and_moderate_power_enabled`: INT,
        `unsolicited_external_voltage_sampling_enabled`: INT,
        `unsolicited_external_voltage_sample_period_ms`: BIGINT,
        `publish_wake_on_can_enabled`: INT
      >,
      `disable_ship_mode`: BOOLEAN,
      `salt_spreader_config`: STRUCT<
        `type`: INT,
        `read_period_ms`: BIGINT,
        `log_period_ms`: BIGINT,
        `force_america_active_override`: BOOLEAN,
        `serial_port_override`: STRUCT<
          `baud_rate`: INT,
          `data_bits`: INT,
          `stop_bits`: INT,
          `parity`: INT,
          `set_rs485_bias`: BOOLEAN,
          `rs485_termination_enabled`: BOOLEAN,
          `protocol`: INT,
          `port`: INT
        >,
        `serial_read_timeout_ms`: BIGINT,
        `passive_mode_off_timeout_ms`: BIGINT,
        `units`: INT,
        `verbose_debug_enabled`: BOOLEAN,
        `emulator_config`: STRUCT<
          `passives`: ARRAY<
            STRUCT<
              `period_ms`: DECIMAL(20, 0),
              `data`: BINARY
            >
          >,
          `responses`: ARRAY<
            STRUCT<
              `request`: BINARY,
              `data`: BINARY
            >
          >,
          `generated_responses`: ARRAY<
            STRUCT<
              `type`: INT,
              `start_value`: DECIMAL(20, 0),
              `increment`: DECIMAL(20, 0),
              `period_ms`: DECIMAL(20, 0),
              `on_off_period_ms`: DECIMAL(20, 0),
              `request`: BINARY
            >
          >,
          `enable_physical_emulation`: BOOLEAN,
          `type`: INT
        >,
        `emulator_configs`: ARRAY<
          STRUCT<
            `passives`: ARRAY<
              STRUCT<
                `period_ms`: DECIMAL(20, 0),
                `data`: BINARY
              >
            >,
            `responses`: ARRAY<
              STRUCT<
                `request`: BINARY,
                `data`: BINARY
              >
            >,
            `generated_responses`: ARRAY<
              STRUCT<
                `type`: INT,
                `start_value`: DECIMAL(20, 0),
                `increment`: DECIMAL(20, 0),
                `period_ms`: DECIMAL(20, 0),
                `on_off_period_ms`: DECIMAL(20, 0),
                `request`: BINARY
              >
            >,
            `enable_physical_emulation`: BOOLEAN,
            `type`: INT
          >
        >,
        `use_deprecated_serial_library`: INT,
        `object_stat_collection_timestamp_offset_enabled`: INT
      >,
      `luft`: STRUCT<
        `mode`: INT,
        `setpoint_initial_delay_ms`: DECIMAL(20, 0),
        `setpoint_interval_ms`: DECIMAL(20, 0),
        `advanced_menu_initial_delay_ms`: DECIMAL(20, 0),
        `advanced_menu_interval_ms`: DECIMAL(20, 0),
        `luft_heartbeat_interval_ms`: DECIMAL(20, 0),
        `max_allowed_ms_without_display_frame`: DECIMAL(20, 0),
        `log_status_on_shutdown`: BOOLEAN,
        `can_interface_manager_enabled`: INT
      >,
      `customer_log_config`: STRUCT<
        `enabled`: INT,
        `min_log_interval_ms`: DECIMAL(20, 0),
        `default_log_interval_ms`: DECIMAL(20, 0)
      >,
      `detection_analytics_config`: STRUCT<
        `enabled`: INT,
        `upload_interval_ms`: DECIMAL(20, 0),
        `min_instance_interval_ms`: DECIMAL(20, 0),
        `enable_diagnostics`: INT
      >,
      `touchscreen_config`: STRUCT<`enabled`: INT>,
      `bandwidth_test`: STRUCT<
        `enabled`: INT,
        `min_test_interval_ms`: BIGINT,
        `max_test_interval_ms`: BIGINT,
        `transfer_duration_ms`: BIGINT,
        `maximum_transfer_bytes_each_way`: BIGINT,
        `receive_buffer_size_bytes`: BIGINT,
        `target_test_interval_ms`: BIGINT,
        `send_buffer_size_bytes`: BIGINT
      >,
      `periodic_reboot_config`: STRUCT<
        `reboot_period_hours`: BIGINT,
        `immediate_reboot_nonce`: BIGINT,
        `reboot_block_limit_hours`: BIGINT,
        `reasons_allowed`: ARRAY<
          STRUCT<
            `reason`: INT,
            `request_enabled`: INT
          >
        >
      >,
      `prevent_config_push`: BOOLEAN,
      `delete_large_device_log_db`: BOOLEAN,
      `serial_port_configs`: ARRAY<
        STRUCT<
          `baud_rate`: INT,
          `data_bits`: INT,
          `stop_bits`: INT,
          `parity`: INT,
          `set_rs485_bias`: BOOLEAN,
          `rs485_termination_enabled`: BOOLEAN,
          `protocol`: INT,
          `port`: INT
        >
      >,
      `problem_recovery_config`: STRUCT<
        `disable_recovery_for_problem_types`: ARRAY<INT>
      >,
      `acc_rs232_settings`: STRUCT<`application`: INT>,
      `replay_mode_config`: STRUCT<
        `recordings`: ARRAY<
          STRUCT<
            `sensor_type`: INT,
            `url`: STRING,
            `md5_hex`: STRING,
            `can_replay_config`: STRUCT<
              `can_setup`: ARRAY<
                STRUCT<
                  `source_iface`: STRING,
                  `target_iface`: STRING,
                  `bitrate`: BIGINT
                >
              >,
              `replay_tool`: STRUCT<`address`: STRING>
            >
          >
        >,
        `enabled`: BOOLEAN,
        `source_org_id`: BIGINT,
        `source_device_id`: BIGINT,
        `source_start_unix_ms`: BIGINT,
        `replay_devices`: ARRAY<INT>
      >,
      `hotplug`: STRUCT<`mount_block_device`: INT>,
      `enable_static_ip_urls`: INT,
      `ddd`: STRUCT<
        `enabled`: INT,
        `distraction_confidence_threshold`: FLOAT,
        `min_distraction_duration_ms`: DECIMAL(20, 0),
        `max_distraction_duration_ms`: DECIMAL(20, 0),
        `recovery_duration_ms`: DECIMAL(20, 0),
        `distracted_fraction_threshold`: FLOAT,
        `driver_face_variance_threshold`: FLOAT,
        `enable_low_severity_events`: BOOLEAN,
        `enable_audio_alerts`: BOOLEAN,
        `alert_duration_ms_low`: DECIMAL(20, 0),
        `alert_duration_ms_medium`: DECIMAL(20, 0),
        `alert_duration_ms_high`: DECIMAL(20, 0),
        `delay_between_high_alerts_ms`: DECIMAL(20, 0),
        `harsh_event_debounce_ms`: DECIMAL(20, 0),
        `use_model_heuristics`: INT,
        `use_headpose_image_flip`: BOOLEAN
      >,
      `syslog_retrieval`: STRUCT<`upload_syslogs_before_unix_ms`: DECIMAL(20, 0)>,
      `firmware_upgrade_options`: STRUCT<
        `ignore_upgrade_interface_restriction`: INT,
        `upgrade_rate_limiter_enabled`: INT,
        `upgrade_rate_limit_kbps`: BIGINT,
        `skip_upgrade_if_insufficent_space_enabled`: INT,
        `delete_old_logs_if_insufficient_space_enabled`: INT,
        `reboot_to_vacuum_if_insufficient_space_enabled`: INT,
        `skip_upgrade_if_insufficient_space_v2_enabled`: INT
      >,
      `supra_cable`: STRUCT<
        `mode`: INT,
        `heartbeat_interval_ms`: BIGINT,
        `bitrate`: BIGINT,
        `startup_delay_secs`: BIGINT,
        `can_interface_manager_enabled`: INT
      >,
      `q_r_code_reader`: STRUCT<
        `enabled`: INT,
        `scan_debug_logging`: STRUCT<
          `enabled`: INT,
          `min_points_detected`: BIGINT
        >,
        `auto_disable_i_r_led_while_scanning`: STRUCT<
          `enabled`: INT,
          `min_points_detected`: BIGINT,
          `max_duration_ms`: BIGINT
        >
      >,
      `event_and_data_logging`: ARRAY<
        STRUCT<
          `event_type`: INT,
          `class`: INT,
          `config`: STRUCT<
            `log_event`: BOOLEAN,
            `data_streams`: ARRAY<BIGINT>,
            `data_start_offset_secs`: BIGINT,
            `data_stop_offset_secs`: BIGINT
          >
        >
      >,
      `imud_config`: STRUCT<
        `enabled`: INT,
        `polling_interval_ms`: BIGINT,
        `enable_bus_publishing`: INT,
        `bus_publish_interval_ms`: BIGINT,
        `imu_sampling_rate_hz`: BIGINT
      >,
      `driver_sign_in_alerts`: STRUCT<
        `reminder_interval_ms`: DECIMAL(20, 0),
        `end_assignment_timeout_ms`: DECIMAL(20, 0),
        `enabled_for_n_f_c_card`: INT,
        `enabled_for_q_r_code`: INT,
        `on_trip_reminders_enabled`: INT,
        `max_on_trip_reminder_count`: DECIMAL(20, 0),
        `play_reminders_only_when_engine_on`: INT,
        `always_play_first_reminder`: STRUCT<
          `enabled`: INT,
          `trip_start_timeout_ms`: DECIMAL(20, 0)
        >,
        `persist_state_enabled`: INT
      >,
      `ntp`: STRUCT<
        `hosts`: ARRAY<
          STRUCT<`hostname`: STRING>
        >,
        `ipv4_only_lookup_enabled`: INT,
        `check_interval_secs`: BIGINT,
        `network_connection_poke_enabled`: INT
      >,
      `cloud_backup_config`: STRUCT<
        `enable`: INT,
        `retry_interval_ms`: DECIMAL(20, 0),
        `max_concurrent_uploads`: BIGINT,
        `upload_delay_ms`: DECIMAL(20, 0),
        `retry_limit`: BIGINT,
        `uploads`: ARRAY<
          STRUCT<
            `mode`: STRUCT<
              `upload_mode`: INT,
              `threshold`: DECIMAL(20, 0)
            >,
            `source`: STRUCT<
              `type`: INT,
              `nvr`: STRUCT<
                `camera_device_id`: BIGINT,
                `stream_id`: BIGINT,
                `channel`: INT
              >
            >
          >
        >,
        `upload_request_url`: STRING
      >,
      `max_speed_alert`: STRUCT<
        `enabled`: INT,
        `max_speed_threshold_mk`: BIGINT,
        `min_duration_before_alert_ms`: BIGINT,
        `audio_alert_debounce_ms`: BIGINT,
        `max_consecutive_audio_alerts`: BIGINT,
        `ecu_speed_staleness_threshold_ms`: BIGINT,
        `no_data_reset_timeout_ms`: BIGINT,
        `require_accel_movement_enabled`: INT,
        `ecu_speed_only_mode_enabled`: INT,
        `audio_alert_policy_manager_enabled`: INT
      >,
      `firmware_metrics_config`: STRUCT<
        `enabled`: INT,
        `report_interval_secs`: BIGINT,
        `metric_configs`: ARRAY<
          STRUCT<
            `name`: STRING,
            `enabled`: INT
          >
        >,
        `grpc_stream_enabled`: INT,
        `health_checker`: STRUCT<`enabled`: INT>
      >,
      `octo_thermal_response_config`: STRUCT<
        `overheated2_video_info`: STRUCT<
          `bitrate`: BIGINT,
          `framerate`: BIGINT,
          `codec`: INT,
          `resolution`: INT,
          `bitrate_control`: INT,
          `idr_interval_seconds`: INT,
          `video_transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >,
          `bframes_m_value`: INT
        >,
        `safemode_video_info`: STRUCT<
          `bitrate`: BIGINT,
          `framerate`: BIGINT,
          `codec`: INT,
          `resolution`: INT,
          `bitrate_control`: INT,
          `idr_interval_seconds`: INT,
          `video_transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >,
          `bframes_m_value`: INT
        >
      >,
      `package_config`: STRUCT<
        `packages`: ARRAY<
          STRUCT<
            `path`: STRING,
            `sha1_hex`: STRING
          >
        >
      >,
      `soze_engine_immobilizer_config`: STRUCT<
        `safety_threshold_kmph`: DECIMAL(20, 0),
        `relay_1`: STRUCT<
          `desired_state`: INT,
          `immobilize_on_tamper`: INT,
          `immobilize_on_jamming`: INT,
          `enablement_nonce`: BIGINT,
          `safety_threshold_kmph`: DECIMAL(20, 0),
          `auto_remobilization_enabled`: INT,
          `auto_remobilization_timeout_seconds`: DECIMAL(20, 0),
          `desired_state_changed_at_timestamp_utc_ms`: DECIMAL(20, 0),
          `desired_state_change_timeout_ms`: DECIMAL(20, 0),
          `safety_threshold_kmph_timeout_seconds`: DECIMAL(20, 0),
          `request_uuid`: BINARY
        >,
        `relay_2`: STRUCT<
          `desired_state`: INT,
          `immobilize_on_tamper`: INT,
          `immobilize_on_jamming`: INT,
          `enablement_nonce`: BIGINT,
          `safety_threshold_kmph`: DECIMAL(20, 0),
          `auto_remobilization_enabled`: INT,
          `auto_remobilization_timeout_seconds`: DECIMAL(20, 0),
          `desired_state_changed_at_timestamp_utc_ms`: DECIMAL(20, 0),
          `desired_state_change_timeout_ms`: DECIMAL(20, 0),
          `safety_threshold_kmph_timeout_seconds`: DECIMAL(20, 0),
          `request_uuid`: BINARY
        >,
        `auto_immobilization_allowed_mobile_country_codes`: ARRAY<BIGINT>,
        `enable_auto_immobilization_allowed_mobile_country_codes`: INT,
        `spoofed_mobile_country_code`: BIGINT,
        `on_trip_detection`: INT,
        `on_trip_immobilize_countdown`: INT,
        `enable_auto_immobilization_geo_lock`: INT,
        `log_region_status`: INT,
        `region_status_min_reporting_interval_secs`: BIGINT,
        `geo_lock_max_time_since_last_gps_fix_secs`: BIGINT,
        `geo_lock_max_gps_accuracy_mm`: BIGINT,
        `jamming_manager`: STRUCT<
          `minimum_duration_enabled`: INT,
          `minimum_duration_seconds`: BIGINT
        >,
        `immobilization_hub_requests_enabled`: INT,
        `use_obd_engine_state_enabled`: INT,
        `immobilization_driver_app_requests_enabled`: INT
      >,
      `jamming_aggregator_config`: STRUCT<
        `gps_jamming_detection_enabled`: INT,
        `cellular_jamming_detection_enabled`: INT
      >,
      `motiond_config`: STRUCT<
        `stillness_enabled`: INT,
        `vibration_enabled`: INT
      >,
      `wgfi_config`: STRUCT<
        `enabled`: INT,
        `vin_interval_ms`: BIGINT,
        `odometer_interval_ms`: BIGINT,
        `engine_hours_interval_ms`: BIGINT,
        `ignition_status_interval_ms`: BIGINT,
        `response_timeout_ms`: BIGINT,
        `wgfi_information_min_interval_ms`: BIGINT,
        `serial_port_config`: STRUCT<
          `baud_rate`: INT,
          `data_bits`: INT,
          `stop_bits`: INT,
          `parity`: INT,
          `set_rs485_bias`: BOOLEAN,
          `rs485_termination_enabled`: BOOLEAN,
          `protocol`: INT,
          `port`: INT
        >,
        `obd_engine_state_bus_message_enabled`: INT
      >,
      `startup_config`: STRUCT<
        `unstable_boot_delay`: STRUCT<
          `enable`: INT,
          `positive_detection_startup_delay_ms`: BIGINT,
          `boot_lookback_count`: BIGINT,
          `minimum_no_delay_avg_uptime_per_boot_ms`: BIGINT,
          `minimum_no_delay_last_boot_uptime_ms`: BIGINT,
          `minimum_no_delay_external_millivolts`: BIGINT
        >,
        `qcs603`: STRUCT<`custom_cpu_affinities_enabled`: INT>
      >,
      `qcs603_low_power_config`: STRUCT<
        `battery_shutdown`: STRUCT<
          `enable_battery_threshold_shutdown`: INT,
          `threshold_shutdown_mv`: DECIMAL(20, 0)
        >,
        `battery_charging_enabled`: INT,
        `use_vg_broadcasted_usb_port_current_limit_enabled`: INT
      >,
      `led`: STRUCT<
        `led_satellite_status_enabled`: INT,
        `led_recording_status_enabled`: INT
      >,
      `forward_collision_warning`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `harsh_events_debounce_ms`: BIGINT,
        `audio_alerts_debounce_ms`: BIGINT,
        `window_size_frames`: INT,
        `confidence_threshold`: FLOAT,
        `min_speed_threshold_kmph`: INT,
        `number_of_predictions_to_skip`: INT,
        `model_fps`: INT,
        `audio_alerts_at_night_enabled`: INT
      >,
      `wifi_client`: STRUCT<
        `dns_servers`: ARRAY<STRING>
      >,
      `inward_safety_mlapp_config`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `mobile_usage_config`: STRUCT<
          `enabled`: INT,
          `min_speed_kmph`: BIGINT,
          `audio_alert_debounce_ms`: BIGINT,
          `model_settings`: STRUCT<
            `confidence_threshold`: FLOAT,
            `detection_window_size`: BIGINT,
            `detection_window_violation_threshold`: BIGINT
          >,
          `harsh_event_debounce_ms`: BIGINT,
          `audio_alert_max_debounce_ms`: BIGINT,
          `audio_alert_debounce_backoff_multiplier`: FLOAT,
          `detection_partition`: STRUCT<
            `detection_side`: INT,
            `box_midpoint_cutoff_percentage`: FLOAT,
            `should_filter_detections`: INT
          >,
          `harsh_event_max_debounce_ms`: BIGINT,
          `harsh_event_debounce_backoff_multiplier`: FLOAT
        >,
        `seatbelt_usage_config`: STRUCT<
          `enabled`: INT,
          `min_speed_kmph`: BIGINT,
          `audio_alert_debounce_ms`: BIGINT,
          `model_settings`: STRUCT<
            `confidence_threshold`: FLOAT,
            `detection_window_size`: BIGINT,
            `detection_window_violation_threshold`: BIGINT
          >,
          `harsh_event_debounce_ms`: BIGINT,
          `audio_alert_max_debounce_ms`: BIGINT,
          `audio_alert_debounce_backoff_multiplier`: FLOAT,
          `detection_partition`: STRUCT<
            `detection_side`: INT,
            `box_midpoint_cutoff_percentage`: FLOAT,
            `should_filter_detections`: INT
          >,
          `harsh_event_max_debounce_ms`: BIGINT,
          `harsh_event_debounce_backoff_multiplier`: FLOAT
        >,
        `inattentive_driving_config`: STRUCT<
          `enabled`: INT,
          `min_speed_kmph`: BIGINT,
          `enable_low_severity_events`: BOOLEAN,
          `delay_before_low_severity_alert_ms`: BIGINT,
          `delay_before_medium_severity_alert_ms`: BIGINT,
          `delay_before_high_severity_alert_ms`: BIGINT,
          `delay_between_high_severity_alerts_ms`: BIGINT,
          `model_settings`: STRUCT<
            `confidence_threshold`: FLOAT,
            `detection_window_size`: BIGINT,
            `detection_window_violation_threshold`: BIGINT,
            `min_distraction_duration_ms`: BIGINT,
            `max_distraction_duration_ms`: BIGINT,
            `recovery_duration_ms`: BIGINT
          >,
          `harsh_event_debounce_ms`: BIGINT,
          `num_consecutive_alerts_before_pausing_detection`: BIGINT,
          `detection_pause_duration_ms`: BIGINT,
          `detection_partition`: STRUCT<
            `detection_side`: INT,
            `box_midpoint_cutoff_percentage`: FLOAT,
            `should_filter_detections`: INT
          >
        >,
        `camera_id_config`: STRUCT<
          `enabled`: INT,
          `jpeg_quality`: BIGINT,
          `detection_partition`: STRUCT<
            `detection_side`: INT,
            `box_midpoint_cutoff_percentage`: FLOAT,
            `should_filter_detections`: INT
          >,
          `shadow_mode_enabled`: INT,
          `model_settings`: STRUCT<`confidence_threshold`: FLOAT>
        >
      >,
      `dashcam_manager_config`: STRUCT<
        `cm_emergency_downloader_mode_recovery_config`: STRUCT<
          `emergency_downloader_mode_usb_reset_enabled`: INT,
          `emergency_downloader_mode_usb_reset_interval_ms`: BIGINT,
          `emergency_downloader_mode_usb_reset_duration_ms`: BIGINT,
          `emergency_downloader_mode_usb_reset_max_times`: BIGINT,
          `emergency_downloader_mode_sahara_reset_enabled`: INT,
          `golang_sahara_reset_implementation_enabled`: INT
        >,
        `recording_trigger`: STRUCT<
          `movement_enabled`: INT,
          `trip_state_enabled`: INT
        >,
        `usb_port_current_limit_override_config`: STRUCT<
          `product_id`: BIGINT,
          `current_limit_milliamps`: BIGINT
        >,
        `usb_reset_config`: STRUCT<
          `enabled`: INT,
          `interval_between_resets_ms`: BIGINT,
          `reset_duration_ms`: BIGINT,
          `max_resets`: BIGINT
        >
      >,
      `evdev_button_manager_config`: STRUCT<
        `cooldown_duration_ms`: BIGINT,
        `long_press_enabled`: INT,
        `double_press_enabled`: INT,
        `double_press_max_gap_ms`: BIGINT,
        `long_press_min_hold_ms`: BIGINT
      >,
      `cable_id`: STRUCT<
        `cable_id_persistence_on_zero_enabled`: INT,
        `cable_id_persistence_on_error_enabled`: INT,
        `cable_id_persistence_on_battery_enabled`: INT
      >,
      `outward_safety_mlapp`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `lane_departure_warning_config`: STRUCT<
          `harsh_events_debounce_ms`: BIGINT,
          `audio_alerts_debounce_ms`: BIGINT,
          `window_size_frames`: INT,
          `confidence_threshold`: FLOAT,
          `min_speed_threshold_kmph`: INT,
          `number_of_predictions_to_skip`: INT,
          `enabled`: INT,
          `white_solid_lane_departure_enabled`: INT,
          `yellow_solid_lane_departure_enabled`: INT
        >,
        `following_distance_config`: STRUCT<
          `enabled`: INT,
          `min_speed_threshold_kmph`: INT,
          `harsh_events_debounce_ms`: BIGINT,
          `audio_alerts_debounce_ms`: BIGINT,
          `unsafe_following_distance_seconds`: FLOAT,
          `unsafe_following_distance_audio_alert_duration_seconds`: FLOAT,
          `unsafe_following_distance_harsh_event_duration_seconds`: FLOAT,
          `confidence_threshold`: FLOAT,
          `min_valid_distance_threshold_meters`: FLOAT,
          `majority_voting_percentile`: FLOAT,
          `min_percentage_of_detections`: FLOAT,
          `distance_seconds_precision_recall_multiplier`: FLOAT,
          `not_tailgating_duration_seconds`: FLOAT
        >,
        `model_fps`: INT
      >,
      `cmapps_button_manager_config`: STRUCT<
        `single_press`: STRUCT<
          `recording_enabled`: INT,
          `recording_duration_ms`: BIGINT
        >,
        `throttle_config`: STRUCT<
          `throttle_enabled`: INT,
          `ignore_button_press_threshold_ms`: BIGINT
        >,
        `enabled`: INT
      >,
      `amba_ml_power`: STRUCT<`enabled`: INT>,
      `dnsmasq_config`: STRUCT<
        `fast_check_enabled`: INT,
        `fast_check_duration_secs`: BIGINT,
        `fast_check_interval_secs`: BIGINT,
        `low_power_steady_state_check_interval_secs`: BIGINT,
        `standard_steady_state_check_interval_secs`: BIGINT,
        `stop_dns_rebind_enabled`: INT,
        `hotspot_dnsmasq_in_low_power_enabled`: INT,
        `identify_dnsmasq_by_pidfiles_enabled`: INT,
        `restart_dnsmasq_on_cell_connect_enabled`: INT
      >,
      `drowsiness_mlapp_config`: STRUCT<
        `ml_app_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `harsh_event_debounce_ms`: DECIMAL(20, 0),
        `audio_alert_debounce_ms`: DECIMAL(20, 0),
        `severity_thresholds`: STRUCT<
          `slightly_drowsy_threshold`: FLOAT,
          `moderately_drowsy_threshold`: FLOAT,
          `very_drowsy_threshold`: FLOAT,
          `extremely_drowsy_threshold`: FLOAT
        >,
        `window_size_frames`: BIGINT,
        `harsh_event_threshold_severity`: INT,
        `model_fps`: BIGINT,
        `number_of_predictions_to_skip`: BIGINT,
        `min_speed_threshold_kmph`: BIGINT,
        `aggregation_multipliers`: STRUCT<
          `unknown_multiplier`: STRUCT<`value`: FLOAT>,
          `not_drowsy_multiplier`: STRUCT<`value`: FLOAT>,
          `slightly_drowsy_multiplier`: STRUCT<`value`: FLOAT>,
          `moderately_drowsy_multiplier`: STRUCT<`value`: FLOAT>,
          `very_drowsy_multiplier`: STRUCT<`value`: FLOAT>,
          `extremely_drowsy_multiplier`: STRUCT<`value`: FLOAT>
        >,
        `frame_black_out`: STRUCT<
          `black_out_side`: INT,
          `black_out_fraction`: FLOAT
        >,
        `enable_audio_alert_decoupling`: INT
      >,
      `bendix_config`: STRUCT<
        `only_drop_rx_data_until_next_packet_enabled`: INT,
        `nack_on_bad_sequence_number_enabled`: INT,
        `nack_on_bad_ingress_data_checksum_enabled`: INT,
        `nack_on_ingress_timeout_enabled`: INT,
        `nack_on_ingress_timeout_ms`: BIGINT,
        `drop_rx_data_on_ingress_timeout_enabled`: INT,
        `max_message_length_bytes`: BIGINT
      >,
      `camera_debug_server`: STRUCT<`enabled`: INT>,
      `carb_config`: STRUCT<
        `carb_session_scheduler`: STRUCT<
          `session_scheduling_enabled`: INT,
          `run_carb_session_message_id`: DECIMAL(20, 0)
        >,
        `carb_session_run`: STRUCT<
          `allowed_obd_compliance_values`: BINARY,
          `wait_for_engine_idle_timeout_ms`: BIGINT,
          `carb_compliant_ecus_request_timeout_ms`: BIGINT,
          `maximum_test_attempts_allowed_count`: BIGINT,
          `maximum_retries_per_request`: BIGINT,
          `carb_ecu_request_pgn_overrides`: ARRAY<BIGINT>,
          `carb_ecu_broadcast_pgn_overrides`: ARRAY<BIGINT>,
          `carb_ecu_required_pgns`: ARRAY<BIGINT>,
          `can_timestamp_config`: STRUCT<
            `timestamping_enabled`: INT,
            `timestamping_max_resolution_ms`: BIGINT
          >,
          `j1979_check_vehicle_status_interval_ms`: BIGINT,
          `carb_ecu_mode01_pids`: ARRAY<DECIMAL(20, 0)>,
          `carb_ecu_mode02_pids`: ARRAY<DECIMAL(20, 0)>,
          `carb_ecu_mode06_mids`: ARRAY<DECIMAL(20, 0)>,
          `carb_ecu_mode09_info_types`: ARRAY<DECIMAL(20, 0)>,
          `j1979_min_response_delay_ms`: BIGINT,
          `j1979_max_response_delay_ms`: BIGINT,
          `j1979_max_stale_vehicle_activity_time_ms`: BIGINT
        >,
        `obd_engine_state_bus_message_enabled`: INT,
        `allowed_can_bus_type`: INT
      >,
      `ir_led`: STRUCT<`override_mode`: INT>,
      `message_bus_archiver`: STRUCT<
        `enable`: INT,
        `denied_message_types`: ARRAY<BIGINT>
      >,
      `equipment_activity`: STRUCT<
        `bus_publish_enabled`: INT,
        `bus_publish_period_ms`: BIGINT,
        `object_stat_publish_enabled`: INT,
        `object_stat_publish_period_ms`: BIGINT,
        `use_obd_engine_state_enabled`: INT,
        `use_aux_input_signal_enabled`: INT,
        `aux_input_signal_id`: INT,
        `always_on_enabled`: INT,
        `aux_input_is_active_low`: INT,
        `log_debug_stat_enabled`: INT,
        `use_voltage_spectrum_activity_state_enabled`: INT
      >,
      `camera_recording_config`: STRUCT<
        `operating_mode`: INT,
        `legacy_policy`: STRUCT<`movement_dwell_ms`: DECIMAL(20, 0)>,
        `publish_camera_target_state_enabled`: INT,
        `engine_voltage_movement_policy`: STRUCT<
          `movement_dwell_ms`: DECIMAL(20, 0),
          `ignition_trigger_wire_enabled`: INT
        >,
        `in_cab_voice_command_policy`: STRUCT<`recording_duration_ms`: DECIMAL(20, 0)>,
        `aux_input_policy`: STRUCT<
          `enabled`: INT,
          `aux_input_1`: STRUCT<
            `enabled`: INT,
            `reason`: INT,
            `recording_duration_ms`: DECIMAL(20, 0)
          >,
          `aux_input_2`: STRUCT<
            `enabled`: INT,
            `reason`: INT,
            `recording_duration_ms`: DECIMAL(20, 0)
          >,
          `aux_input_3`: STRUCT<
            `enabled`: INT,
            `reason`: INT,
            `recording_duration_ms`: DECIMAL(20, 0)
          >,
          `aux_input_4`: STRUCT<
            `enabled`: INT,
            `reason`: INT,
            `recording_duration_ms`: DECIMAL(20, 0)
          >,
          `aux_input_5`: STRUCT<
            `enabled`: INT,
            `reason`: INT,
            `recording_duration_ms`: DECIMAL(20, 0)
          >
        >
      >,
      `low_bridge_strike`: STRUCT<
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
      `hardware_info`: STRUCT<`logging_enabled`: INT>,
      `still_manager`: STRUCT<`operating_mode`: INT>,
      `memory_test_config`: STRUCT<
        `log_results_object_stat_enabled`: INT,
        `run_mem_test_if_no_results_enabled`: INT,
        `led_status_enabled`: INT,
        `run_repeat_mem_test_enabled`: INT
      >,
      `inward_safety_mlapp_service_b_config`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `mobile_usage_config`: STRUCT<
          `enabled`: INT,
          `min_speed_kmph`: BIGINT,
          `audio_alert_debounce_ms`: BIGINT,
          `model_settings`: STRUCT<
            `confidence_threshold`: FLOAT,
            `detection_window_size`: BIGINT,
            `detection_window_violation_threshold`: BIGINT
          >,
          `harsh_event_debounce_ms`: BIGINT,
          `audio_alert_max_debounce_ms`: BIGINT,
          `audio_alert_debounce_backoff_multiplier`: FLOAT,
          `detection_partition`: STRUCT<
            `detection_side`: INT,
            `box_midpoint_cutoff_percentage`: FLOAT,
            `should_filter_detections`: INT
          >,
          `harsh_event_max_debounce_ms`: BIGINT,
          `harsh_event_debounce_backoff_multiplier`: FLOAT
        >,
        `seatbelt_usage_config`: STRUCT<
          `enabled`: INT,
          `min_speed_kmph`: BIGINT,
          `audio_alert_debounce_ms`: BIGINT,
          `model_settings`: STRUCT<
            `confidence_threshold`: FLOAT,
            `detection_window_size`: BIGINT,
            `detection_window_violation_threshold`: BIGINT
          >,
          `harsh_event_debounce_ms`: BIGINT,
          `audio_alert_max_debounce_ms`: BIGINT,
          `audio_alert_debounce_backoff_multiplier`: FLOAT,
          `detection_partition`: STRUCT<
            `detection_side`: INT,
            `box_midpoint_cutoff_percentage`: FLOAT,
            `should_filter_detections`: INT
          >,
          `harsh_event_max_debounce_ms`: BIGINT,
          `harsh_event_debounce_backoff_multiplier`: FLOAT
        >,
        `inattentive_driving_config`: STRUCT<
          `enabled`: INT,
          `min_speed_kmph`: BIGINT,
          `enable_low_severity_events`: BOOLEAN,
          `delay_before_low_severity_alert_ms`: BIGINT,
          `delay_before_medium_severity_alert_ms`: BIGINT,
          `delay_before_high_severity_alert_ms`: BIGINT,
          `delay_between_high_severity_alerts_ms`: BIGINT,
          `model_settings`: STRUCT<
            `confidence_threshold`: FLOAT,
            `detection_window_size`: BIGINT,
            `detection_window_violation_threshold`: BIGINT,
            `min_distraction_duration_ms`: BIGINT,
            `max_distraction_duration_ms`: BIGINT,
            `recovery_duration_ms`: BIGINT
          >,
          `harsh_event_debounce_ms`: BIGINT,
          `num_consecutive_alerts_before_pausing_detection`: BIGINT,
          `detection_pause_duration_ms`: BIGINT,
          `detection_partition`: STRUCT<
            `detection_side`: INT,
            `box_midpoint_cutoff_percentage`: FLOAT,
            `should_filter_detections`: INT
          >
        >,
        `camera_id_config`: STRUCT<
          `enabled`: INT,
          `jpeg_quality`: BIGINT,
          `detection_partition`: STRUCT<
            `detection_side`: INT,
            `box_midpoint_cutoff_percentage`: FLOAT,
            `should_filter_detections`: INT
          >,
          `shadow_mode_enabled`: INT,
          `model_settings`: STRUCT<`confidence_threshold`: FLOAT>
        >
      >,
      `outward_safety_mlapp_service_b_config`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `lane_departure_warning_config`: STRUCT<
          `harsh_events_debounce_ms`: BIGINT,
          `audio_alerts_debounce_ms`: BIGINT,
          `window_size_frames`: INT,
          `confidence_threshold`: FLOAT,
          `min_speed_threshold_kmph`: INT,
          `number_of_predictions_to_skip`: INT,
          `enabled`: INT,
          `white_solid_lane_departure_enabled`: INT,
          `yellow_solid_lane_departure_enabled`: INT
        >,
        `following_distance_config`: STRUCT<
          `enabled`: INT,
          `min_speed_threshold_kmph`: INT,
          `harsh_events_debounce_ms`: BIGINT,
          `audio_alerts_debounce_ms`: BIGINT,
          `unsafe_following_distance_seconds`: FLOAT,
          `unsafe_following_distance_audio_alert_duration_seconds`: FLOAT,
          `unsafe_following_distance_harsh_event_duration_seconds`: FLOAT,
          `confidence_threshold`: FLOAT,
          `min_valid_distance_threshold_meters`: FLOAT,
          `majority_voting_percentile`: FLOAT,
          `min_percentage_of_detections`: FLOAT,
          `distance_seconds_precision_recall_multiplier`: FLOAT,
          `not_tailgating_duration_seconds`: FLOAT
        >,
        `model_fps`: INT
      >,
      `message_bus_proxy_manager_config`: STRUCT<
        `time_sync_enabled`: INT,
        `bus_proxy_v2_enabled`: INT
      >,
      `satellite_manager_config`: STRUCT<
        `enabled`: INT,
        `periodic_message_interval_secs`: BIGINT,
        `max_modem_power_cycles`: BIGINT,
        `send_messages_with_connectivity_enabled`: INT,
        `status_logging_enabled`: INT,
        `status_log_interval_secs`: BIGINT,
        `hub_disconnect_delay_secs`: BIGINT,
        `always_send_message_after_boot_enabled`: INT,
        `send_message_on_trip_change_enabled`: INT,
        `force_use_protocol_version`: BIGINT,
        `security`: STRUCT<
          `base_encryption_key`: BINARY,
          `base_salt`: BINARY,
          `key_identifier`: BIGINT,
          `unique_id`: BIGINT,
          `send_unique_id_per_eight_messages_enabled`: INT,
          `send_unique_id_per_eight_messages`: BIGINT,
          `send_unique_id_for_messages_after_imei_change`: BIGINT
        >,
        `panic_button_messages_enabled`: INT,
        `panic_button_rate_limit_seconds`: BIGINT,
        `send_panic_button_after_boot_enabled`: INT,
        `num_panic_button_messages_to_send_after_boot`: BIGINT,
        `publish_satellite_status_enabled`: INT
      >,
      `signdetection_mlapp`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `model_fps`: BIGINT,
        `confidence_threshold`: FLOAT,
        `detection_long_enough_duration_ms`: BIGINT,
        `harsh_event_debounce_ms`: BIGINT,
        `object_class_detection_configs`: ARRAY<
          STRUCT<
            `confidence_threshold`: FLOAT,
            `detection_long_enough_duration_ms`: BIGINT,
            `output_tensor_index`: BIGINT
          >
        >,
        `skip_bus_message`: BOOLEAN,
        `model_output_config`: STRUCT<
          `bbox_width_norm_factor`: FLOAT,
          `bbox_height_norm_factor`: FLOAT,
          `enable_nms`: BOOLEAN,
          `nms`: STRUCT<
            `max_iou_threshold`: FLOAT,
            `top_k`: INT,
            `max_count`: INT
          >,
          `filter`: STRUCT<
            `min_score_threshold`: FLOAT,
            `filter_label_ids`: ARRAY<INT>,
            `filter_tensor_names`: ARRAY<STRING>
          >
        >,
        `traffic_light_detection`: STRUCT<
          `enabled`: INT,
          `governing_traffic_light_detection`: STRUCT<
            `confidence_threshold`: FLOAT,
            `min_detection_duration_ms`: BIGINT
          >,
          `harsh_event_debounce_ms`: BIGINT
        >,
        `second_model_output_config`: STRUCT<
          `bbox_width_norm_factor`: FLOAT,
          `bbox_height_norm_factor`: FLOAT,
          `enable_nms`: BOOLEAN,
          `nms`: STRUCT<
            `max_iou_threshold`: FLOAT,
            `top_k`: INT,
            `max_count`: INT
          >,
          `filter`: STRUCT<
            `min_score_threshold`: FLOAT,
            `filter_label_ids`: ARRAY<INT>,
            `filter_tensor_names`: ARRAY<STRING>
          >
        >
      >,
      `harsh_event_metadata_config`: STRUCT<
        `metadata_time_ranges`: ARRAY<
          STRUCT<
            `event_type`: INT,
            `pre_event_offset_ms`: BIGINT,
            `post_event_offset_ms`: BIGINT
          >
        >
      >,
      `owl_log_config`: STRUCT<
        `default_log_level`: STRING,
        `attribute_config`: ARRAY<
          STRUCT<
            `attribute_key`: STRING,
            `attribute_value`: STRING,
            `log_level`: STRING
          >
        >
      >,
      `signal_sampler`: STRUCT<
        `enabled`: INT,
        `sample_selection_period_ms`: BIGINT,
        `sample_start_probability`: FLOAT,
        `sample_duration_ms`: BIGINT,
        `precondition_max_total_cpu_percent_enabled`: INT,
        `precondition_max_total_cpu_percent`: FLOAT,
        `can_sampler`: STRUCT<
          `enabled`: INT,
          `sample_selection_period_ms`: BIGINT,
          `sample_start_probability`: FLOAT,
          `sample_duration_ms`: BIGINT,
          `sample_period_ms`: BIGINT,
          `precondition_max_total_cpu_percent_enabled`: INT,
          `precondition_max_total_cpu_percent`: FLOAT,
          `precondition_allowed_trip_statuses_enabled`: INT,
          `precondition_allowed_trip_statuses`: ARRAY<INT>,
          `precondition_allowed_power_states_enabled`: INT,
          `precondition_allowed_power_states`: ARRAY<INT>
        >,
        `external_voltage_sampler`: STRUCT<
          `enabled`: INT,
          `sample_selection_period_ms`: BIGINT,
          `sample_start_probability`: FLOAT,
          `sample_duration_ms`: BIGINT,
          `sample_period_ms`: BIGINT,
          `precondition_max_total_cpu_percent_enabled`: INT,
          `precondition_max_total_cpu_percent`: FLOAT,
          `precondition_allowed_trip_statuses_enabled`: INT,
          `precondition_allowed_trip_statuses`: ARRAY<INT>,
          `precondition_allowed_power_states_enabled`: INT,
          `precondition_allowed_power_states`: ARRAY<INT>
        >
      >,
      `config_reporting`: STRUCT<`sanitize_sensitive_info_enabled`: INT>,
      `banshee_storage_manager`: STRUCT<`enabled`: INT>,
      `vulnerable_road_user_collision_warning_mlapp_config`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `front_vrucw_config`: STRUCT<
          `enabled`: INT,
          `media_inputs`: ARRAY<INT>,
          `multi_tracker_config`: STRUCT<
            `distance_threshold`: FLOAT,
            `distance_fn_weights`: STRUCT<
              `iou_weight`: FLOAT,
              `cosine_weight`: FLOAT
            >,
            `ms_ahead`: BIGINT,
            `max_age_ms`: BIGINT
          >,
          `harsh_events_debounce_ms`: BIGINT,
          `audio_alerts_debounce_ms`: BIGINT,
          `min_speed_threshold_kmph`: INT,
          `max_speed_threshold_kmph`: INT,
          `ego_motion_threshold`: FLOAT,
          `activation_triggers`: ARRAY<INT>,
          `decision_logic_cfg`: STRUCT<
            `roi_config`: STRUCT<
              `is_class_labels_for_inclusion`: BOOLEAN,
              `class_labels`: ARRAY<BIGINT>
            >,
            `enable_bbox_filter`: BOOLEAN,
            `bbox_filters`: ARRAY<
              STRUCT<
                `class_index`: BIGINT,
                `height_threshold`: FLOAT,
                `width_threshold`: FLOAT
              >
            >,
            `severity_score_threshold`: STRUCT<
              `low_severity_score_threshold`: FLOAT,
              `high_severity_score_threshold`: FLOAT
            >
          >
        >,
        `rear_vrucw_config`: STRUCT<
          `enabled`: INT,
          `media_inputs`: ARRAY<INT>,
          `multi_tracker_config`: STRUCT<
            `distance_threshold`: FLOAT,
            `distance_fn_weights`: STRUCT<
              `iou_weight`: FLOAT,
              `cosine_weight`: FLOAT
            >,
            `ms_ahead`: BIGINT,
            `max_age_ms`: BIGINT
          >,
          `harsh_events_debounce_ms`: BIGINT,
          `audio_alerts_debounce_ms`: BIGINT,
          `ego_motion_threshold`: FLOAT,
          `min_speed_threshold_kmph`: INT,
          `max_speed_threshold_kmph`: INT,
          `activation_triggers`: ARRAY<INT>,
          `decision_logic_cfg`: STRUCT<
            `roi_config`: STRUCT<
              `is_class_labels_for_inclusion`: BOOLEAN,
              `class_labels`: ARRAY<BIGINT>
            >,
            `enable_bbox_filter`: BOOLEAN,
            `bbox_filters`: ARRAY<
              STRUCT<
                `class_index`: BIGINT,
                `height_threshold`: FLOAT,
                `width_threshold`: FLOAT
              >
            >,
            `severity_score_threshold`: STRUCT<
              `low_severity_score_threshold`: FLOAT,
              `high_severity_score_threshold`: FLOAT
            >
          >
        >,
        `left_vrucw_config`: STRUCT<
          `enabled`: INT,
          `media_inputs`: ARRAY<INT>,
          `multi_tracker_config`: STRUCT<
            `distance_threshold`: FLOAT,
            `distance_fn_weights`: STRUCT<
              `iou_weight`: FLOAT,
              `cosine_weight`: FLOAT
            >,
            `ms_ahead`: BIGINT,
            `max_age_ms`: BIGINT
          >,
          `harsh_events_debounce_ms`: BIGINT,
          `audio_alerts_debounce_ms`: BIGINT,
          `min_speed_threshold_kmph`: INT,
          `max_speed_threshold_kmph`: INT,
          `activation_triggers`: ARRAY<INT>,
          `decision_logic_cfg`: STRUCT<
            `roi_config`: STRUCT<
              `is_class_labels_for_inclusion`: BOOLEAN,
              `class_labels`: ARRAY<BIGINT>
            >,
            `enable_bbox_filter`: BOOLEAN,
            `bbox_filters`: ARRAY<
              STRUCT<
                `class_index`: BIGINT,
                `height_threshold`: FLOAT,
                `width_threshold`: FLOAT
              >
            >,
            `severity_score_threshold`: STRUCT<
              `low_severity_score_threshold`: FLOAT,
              `high_severity_score_threshold`: FLOAT
            >
          >
        >,
        `right_vrucw_config`: STRUCT<
          `enabled`: INT,
          `media_inputs`: ARRAY<INT>,
          `multi_tracker_config`: STRUCT<
            `distance_threshold`: FLOAT,
            `distance_fn_weights`: STRUCT<
              `iou_weight`: FLOAT,
              `cosine_weight`: FLOAT
            >,
            `ms_ahead`: BIGINT,
            `max_age_ms`: BIGINT
          >,
          `harsh_events_debounce_ms`: BIGINT,
          `audio_alerts_debounce_ms`: BIGINT,
          `min_speed_threshold_kmph`: INT,
          `max_speed_threshold_kmph`: INT,
          `activation_triggers`: ARRAY<INT>,
          `decision_logic_cfg`: STRUCT<
            `roi_config`: STRUCT<
              `is_class_labels_for_inclusion`: BOOLEAN,
              `class_labels`: ARRAY<BIGINT>
            >,
            `enable_bbox_filter`: BOOLEAN,
            `bbox_filters`: ARRAY<
              STRUCT<
                `class_index`: BIGINT,
                `height_threshold`: FLOAT,
                `width_threshold`: FLOAT
              >
            >,
            `severity_score_threshold`: STRUCT<
              `low_severity_score_threshold`: FLOAT,
              `high_severity_score_threshold`: FLOAT
            >
          >
        >,
        `model_fps`: INT,
        `model_output_config`: STRUCT<
          `bbox_width_norm_factor`: FLOAT,
          `bbox_height_norm_factor`: FLOAT,
          `enable_nms`: BOOLEAN,
          `nms`: STRUCT<
            `max_iou_threshold`: FLOAT,
            `top_k`: INT,
            `max_count`: INT
          >,
          `filter`: STRUCT<
            `min_score_threshold`: FLOAT,
            `filter_label_ids`: ARRAY<INT>,
            `filter_tensor_names`: ARRAY<STRING>
          >
        >,
        `severity_score_threshold`: FLOAT,
        `window_size_frames`: INT,
        `window_size_threshold`: BIGINT,
        `events_enabled`: INT,
        `audio_alerts_enabled`: INT,
        `gst_motion_estimation_config`: STRUCT<
          `lk_max_level`: BIGINT,
          `lk_term_criteria_epsilon`: FLOAT,
          `lk_term_criteria_max_count`: BIGINT,
          `lk_term_criteria_type`: INT,
          `lk_window_height`: BIGINT,
          `lk_window_width`: BIGINT,
          `momentum`: FLOAT,
          `max_estimation_gap_ms`: BIGINT
        >,
        `enable_overlay`: INT,
        `disable_low_severity_events`: INT,
        `rear_collision_warning_config`: STRUCT<
          `enabled`: INT,
          `decision_logic_config`: STRUCT<
            `grid_config`: STRUCT<
              `rows`: BIGINT,
              `cols`: BIGINT
            >,
            `activation_ratio_thresh`: FLOAT,
            `min_pixels_per_cell`: BIGINT,
            `thresholds_per_cell`: ARRAY<FLOAT>,
            `exclude_classes`: ARRAY<BIGINT>,
            `roi_corners`: ARRAY<
              STRUCT<
                `x`: FLOAT,
                `y`: FLOAT
              >
            >
          >,
          `window_aggregator_config`: STRUCT<
            `window_size`: BIGINT,
            `window_threshold`: BIGINT
          >,
          `events_enabled`: INT,
          `audio_alerts_enabled`: INT,
          `harsh_events_debounce_ms`: BIGINT,
          `audio_alerts_debounce_ms`: BIGINT
        >,
        `vehicle_in_blind_spot_warning_config`: STRUCT<
          `enabled`: INT,
          `enable_audio_alerts`: INT,
          `enable_overlays`: INT,
          `harsh_events_debounce_ms`: BIGINT,
          `audio_alerts_debounce_ms`: BIGINT,
          `min_speed_threshold_kmph`: INT,
          `severity_score_threshold`: FLOAT,
          `window_aggregator_config`: STRUCT<
            `window_size`: BIGINT,
            `window_threshold`: BIGINT
          >,
          `left_vbs_config`: STRUCT<
            `roi_points`: ARRAY<
              STRUCT<
                `x`: FLOAT,
                `y`: FLOAT
              >
            >
          >,
          `right_vbs_config`: STRUCT<
            `roi_points`: ARRAY<
              STRUCT<
                `x`: FLOAT,
                `y`: FLOAT
              >
            >
          >
        >
      >,
      `eld_service_manager_config`: STRUCT<
        `enabled`: INT,
        `allowed_mcc_override`: ARRAY<BIGINT>,
        `latitude_offset_nanodeg_override`: BIGINT,
        `longitude_offset_nanodeg_override`: BIGINT,
        `commands_processing_enabled`: INT,
        `debug_mode_enabled`: INT,
        `engine_sync_dm`: STRUCT<
          `engine_sync_delay_window_seconds`: BIGINT,
          `additional_engine_sync_delay_window_after_engine_on_seconds`: BIGINT,
          `infer_engine_on_voltage_spread_mv`: BIGINT,
          `voltage_buffer_unreliability_spread_mv`: BIGINT,
          `voltage_buffer_sample_size`: BIGINT,
          `voltage_buffer_sample_interval_seconds`: BIGINT,
          `voltage_based_engine_sync_enabled`: INT,
          `all_ecm_signals_stale_threshold_seconds`: BIGINT,
          `any_ecm_signal_stale_threshold_seconds`: BIGINT,
          `use_gps_based_motion`: INT,
          `gps_based_motion_threshold_kmph`: BIGINT,
          `gps_datapoint_validity_window_seconds`: BIGINT
        >,
        `eld_enabled`: INT,
        `regulation_mode`: INT,
        `license_check_enabled`: INT,
        `license_expires_at_utc_ms`: DECIMAL(20, 0),
        `power_dm`: STRUCT<
          `enabled`: INT,
          `well_known_engine_seconds_increments`: ARRAY<BIGINT>,
          `clear_diagnostic_after_seconds`: BIGINT
        >,
        `carrier_timezone`: STRING,
        `unidentified_driving_dm`: STRUCT<`enabled`: INT>,
        `unidentified_driving_alert`: STRUCT<
          `enabled`: INT,
          `repeat_interval_seconds`: BIGINT
        >,
        `yard_move`: STRUCT<
          `persist_after_engine_restart`: BOOLEAN,
          `auto_transition_enabled`: BOOLEAN,
          `entry_prompt_enabled`: BOOLEAN
        >
      >,
      `forward_collision_warning_service_b_config`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `harsh_events_debounce_ms`: BIGINT,
        `audio_alerts_debounce_ms`: BIGINT,
        `window_size_frames`: INT,
        `confidence_threshold`: FLOAT,
        `min_speed_threshold_kmph`: INT,
        `number_of_predictions_to_skip`: INT,
        `model_fps`: INT,
        `audio_alerts_at_night_enabled`: INT
      >,
      `drowsiness_mlapp_service_b_config`: STRUCT<
        `ml_app_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `harsh_event_debounce_ms`: DECIMAL(20, 0),
        `audio_alert_debounce_ms`: DECIMAL(20, 0),
        `severity_thresholds`: STRUCT<
          `slightly_drowsy_threshold`: FLOAT,
          `moderately_drowsy_threshold`: FLOAT,
          `very_drowsy_threshold`: FLOAT,
          `extremely_drowsy_threshold`: FLOAT
        >,
        `window_size_frames`: BIGINT,
        `harsh_event_threshold_severity`: INT,
        `model_fps`: BIGINT,
        `number_of_predictions_to_skip`: BIGINT,
        `min_speed_threshold_kmph`: BIGINT,
        `aggregation_multipliers`: STRUCT<
          `unknown_multiplier`: STRUCT<`value`: FLOAT>,
          `not_drowsy_multiplier`: STRUCT<`value`: FLOAT>,
          `slightly_drowsy_multiplier`: STRUCT<`value`: FLOAT>,
          `moderately_drowsy_multiplier`: STRUCT<`value`: FLOAT>,
          `very_drowsy_multiplier`: STRUCT<`value`: FLOAT>,
          `extremely_drowsy_multiplier`: STRUCT<`value`: FLOAT>
        >,
        `frame_black_out`: STRUCT<
          `black_out_side`: INT,
          `black_out_fraction`: FLOAT
        >,
        `enable_audio_alert_decoupling`: INT
      >,
      `signdetection_mlapp_service_b_config`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `model_fps`: BIGINT,
        `confidence_threshold`: FLOAT,
        `detection_long_enough_duration_ms`: BIGINT,
        `harsh_event_debounce_ms`: BIGINT,
        `object_class_detection_configs`: ARRAY<
          STRUCT<
            `confidence_threshold`: FLOAT,
            `detection_long_enough_duration_ms`: BIGINT,
            `output_tensor_index`: BIGINT
          >
        >,
        `skip_bus_message`: BOOLEAN,
        `model_output_config`: STRUCT<
          `bbox_width_norm_factor`: FLOAT,
          `bbox_height_norm_factor`: FLOAT,
          `enable_nms`: BOOLEAN,
          `nms`: STRUCT<
            `max_iou_threshold`: FLOAT,
            `top_k`: INT,
            `max_count`: INT
          >,
          `filter`: STRUCT<
            `min_score_threshold`: FLOAT,
            `filter_label_ids`: ARRAY<INT>,
            `filter_tensor_names`: ARRAY<STRING>
          >
        >,
        `traffic_light_detection`: STRUCT<
          `enabled`: INT,
          `governing_traffic_light_detection`: STRUCT<
            `confidence_threshold`: FLOAT,
            `min_detection_duration_ms`: BIGINT
          >,
          `harsh_event_debounce_ms`: BIGINT
        >,
        `second_model_output_config`: STRUCT<
          `bbox_width_norm_factor`: FLOAT,
          `bbox_height_norm_factor`: FLOAT,
          `enable_nms`: BOOLEAN,
          `nms`: STRUCT<
            `max_iou_threshold`: FLOAT,
            `top_k`: INT,
            `max_count`: INT
          >,
          `filter`: STRUCT<
            `min_score_threshold`: FLOAT,
            `filter_label_ids`: ARRAY<INT>,
            `filter_tensor_names`: ARRAY<STRING>
          >
        >
      >,
      `trigger_wire`: STRUCT<
        `wire_1`: STRUCT<
          `signal_type`: INT,
          `active_low`: BOOLEAN
        >,
        `wire_2`: STRUCT<
          `signal_type`: INT,
          `active_low`: BOOLEAN
        >,
        `wire_3`: STRUCT<
          `signal_type`: INT,
          `active_low`: BOOLEAN
        >,
        `wire_4`: STRUCT<
          `signal_type`: INT,
          `active_low`: BOOLEAN
        >,
        `wire_5`: STRUCT<
          `signal_type`: INT,
          `active_low`: BOOLEAN
        >
      >,
      `endpoint`: STRUCT<
        `hubserver_domain_prefix`: STRING,
        `hubserver_domain_prefix_enabled`: INT,
        `hubserver_domain_prefix_on_odd_boots_enabled`: INT,
        `region_config_domain_prefix`: STRING,
        `region_config_domain_prefix_enabled`: INT,
        `region_config_domain_prefix_on_odd_boots_enabled`: INT,
        `min_tls_level`: INT,
        `min_tls_level_enabled`: INT,
        `min_tls_level_on_odd_boots_enabled`: INT
      >,
      `device_config_reader`: STRUCT<`read_member_enabled`: INT>,
      `voltage_spectrum_manager`: STRUCT<
        `enabled`: INT,
        `voltage_spectrum_debug_stat_enabled`: INT,
        `power_spectral_density_checker`: STRUCT<
          `enabled`: INT,
          `sample_rate_hz`: BIGINT,
          `window_size_samples`: BIGINT,
          `window_overlap_samples`: BIGINT,
          `high_pass_freq_threshold_min_bin_number`: BIGINT,
          `power_threshold_millivolts_squared_per_hz`: BIGINT,
          `active_window_ms`: BIGINT,
          `inactive_window_ms`: BIGINT,
          `high_power_event_count_threshold`: BIGINT
        >
      >,
      `media_inputs`: STRUCT<
        `primary_video`: STRUCT<
          `transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >
        >,
        `secondary_video`: STRUCT<
          `transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >
        >,
        `analog_1_video`: STRUCT<
          `transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >
        >,
        `analog_2_video`: STRUCT<
          `transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >
        >,
        `analog_3_video`: STRUCT<
          `transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >
        >,
        `analog_4_video`: STRUCT<
          `transform`: STRUCT<
            `flip_vertical`: BOOLEAN,
            `flip_horizontal`: BOOLEAN,
            `rotation`: INT
          >
        >
      >,
      `dashcam_vulnerable_road_user_collision_warning_mlapp_config`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `multi_tracker_config`: STRUCT<
          `distance_threshold`: FLOAT,
          `distance_fn_weights`: STRUCT<
            `iou_weight`: FLOAT,
            `cosine_weight`: FLOAT
          >,
          `ms_ahead`: BIGINT,
          `max_age_ms`: BIGINT
        >,
        `model_output_config`: STRUCT<
          `bbox_width_norm_factor`: FLOAT,
          `bbox_height_norm_factor`: FLOAT,
          `enable_nms`: BOOLEAN,
          `nms`: STRUCT<
            `max_iou_threshold`: FLOAT,
            `top_k`: INT,
            `max_count`: INT
          >,
          `filter`: STRUCT<
            `min_score_threshold`: FLOAT,
            `filter_label_ids`: ARRAY<INT>,
            `filter_tensor_names`: ARRAY<STRING>
          >
        >,
        `model_fps`: INT,
        `min_speed_kmph`: INT,
        `initialization_seconds`: FLOAT,
        `vehicle_line`: ARRAY<FLOAT>,
        `ttc_position_duration_seconds`: FLOAT,
        `ttc_distance_duration_seconds`: FLOAT,
        `harsh_events_debounce_ms`: BIGINT,
        `audio_alerts_debounce_ms`: BIGINT,
        `audio_alerts_enabled`: INT,
        `harsh_events_enabled`: INT,
        `gst_motion_estimation_config`: STRUCT<
          `momentum`: FLOAT,
          `max_estimation_gap_ms`: BIGINT
        >,
        `ego_motion_threshold`: FLOAT,
        `roi_scaling_factor`: FLOAT,
        `severity_score_threshold`: FLOAT,
        `enable_trajectory_severity_score`: BOOLEAN,
        `depth_reward_weight`: FLOAT,
        `depth_penalty_weight`: FLOAT,
        `object_approaching_severity_score`: FLOAT,
        `object_leaving_severity_score`: FLOAT,
        `object_staying_severity_score`: FLOAT,
        `disable_low_severity_events`: BOOLEAN,
        `low_severity_score_threshold`: FLOAT,
        `roi_corners`: ARRAY<
          STRUCT<
            `x`: FLOAT,
            `y`: FLOAT
          >
        >
      >,
      `mlapp_director`: STRUCT<
        `apps`: ARRAY<
          STRUCT<
            `enabled`: INT,
            `name`: STRING,
            `mlapp_config`: BINARY,
            `profile_name`: STRING,
            `cohort_name`: STRING,
            `feature_type`: INT,
            `shadow_mode_enabled`: BOOLEAN,
            `feature_name`: STRING
          >
        >
      >,
      `geofence`: STRUCT<
        `geofence_groups`: ARRAY<
          STRUCT<
            `input_recording_config`: STRUCT<
              `primary`: STRUCT<
                `high_res`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `bitrate`: BIGINT,
                    `framerate`: BIGINT,
                    `codec`: INT,
                    `resolution`: INT,
                    `bitrate_control`: INT,
                    `idr_interval_seconds`: INT,
                    `video_transform`: STRUCT<
                      `flip_vertical`: BOOLEAN,
                      `flip_horizontal`: BOOLEAN,
                      `rotation`: INT
                    >,
                    `bframes_m_value`: INT
                  >
                >,
                `low_res`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `bitrate`: BIGINT,
                    `framerate`: BIGINT,
                    `codec`: INT,
                    `resolution`: INT,
                    `bitrate_control`: INT,
                    `idr_interval_seconds`: INT,
                    `video_transform`: STRUCT<
                      `flip_vertical`: BOOLEAN,
                      `flip_horizontal`: BOOLEAN,
                      `rotation`: INT
                    >,
                    `bframes_m_value`: INT
                  >
                >,
                `audio`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `samplerate`: BIGINT,
                    `audio_codec`: INT
                  >
                >
              >,
              `secondary`: STRUCT<
                `high_res`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `bitrate`: BIGINT,
                    `framerate`: BIGINT,
                    `codec`: INT,
                    `resolution`: INT,
                    `bitrate_control`: INT,
                    `idr_interval_seconds`: INT,
                    `video_transform`: STRUCT<
                      `flip_vertical`: BOOLEAN,
                      `flip_horizontal`: BOOLEAN,
                      `rotation`: INT
                    >,
                    `bframes_m_value`: INT
                  >
                >,
                `low_res`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `bitrate`: BIGINT,
                    `framerate`: BIGINT,
                    `codec`: INT,
                    `resolution`: INT,
                    `bitrate_control`: INT,
                    `idr_interval_seconds`: INT,
                    `video_transform`: STRUCT<
                      `flip_vertical`: BOOLEAN,
                      `flip_horizontal`: BOOLEAN,
                      `rotation`: INT
                    >,
                    `bframes_m_value`: INT
                  >
                >,
                `audio`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `samplerate`: BIGINT,
                    `audio_codec`: INT
                  >
                >
              >,
              `analog_1`: STRUCT<
                `high_res`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `bitrate`: BIGINT,
                    `framerate`: BIGINT,
                    `codec`: INT,
                    `resolution`: INT,
                    `bitrate_control`: INT,
                    `idr_interval_seconds`: INT,
                    `video_transform`: STRUCT<
                      `flip_vertical`: BOOLEAN,
                      `flip_horizontal`: BOOLEAN,
                      `rotation`: INT
                    >,
                    `bframes_m_value`: INT
                  >
                >,
                `low_res`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `bitrate`: BIGINT,
                    `framerate`: BIGINT,
                    `codec`: INT,
                    `resolution`: INT,
                    `bitrate_control`: INT,
                    `idr_interval_seconds`: INT,
                    `video_transform`: STRUCT<
                      `flip_vertical`: BOOLEAN,
                      `flip_horizontal`: BOOLEAN,
                      `rotation`: INT
                    >,
                    `bframes_m_value`: INT
                  >
                >,
                `audio`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `samplerate`: BIGINT,
                    `audio_codec`: INT
                  >
                >
              >,
              `analog_2`: STRUCT<
                `high_res`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `bitrate`: BIGINT,
                    `framerate`: BIGINT,
                    `codec`: INT,
                    `resolution`: INT,
                    `bitrate_control`: INT,
                    `idr_interval_seconds`: INT,
                    `video_transform`: STRUCT<
                      `flip_vertical`: BOOLEAN,
                      `flip_horizontal`: BOOLEAN,
                      `rotation`: INT
                    >,
                    `bframes_m_value`: INT
                  >
                >,
                `low_res`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `bitrate`: BIGINT,
                    `framerate`: BIGINT,
                    `codec`: INT,
                    `resolution`: INT,
                    `bitrate_control`: INT,
                    `idr_interval_seconds`: INT,
                    `video_transform`: STRUCT<
                      `flip_vertical`: BOOLEAN,
                      `flip_horizontal`: BOOLEAN,
                      `rotation`: INT
                    >,
                    `bframes_m_value`: INT
                  >
                >,
                `audio`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `samplerate`: BIGINT,
                    `audio_codec`: INT
                  >
                >
              >,
              `analog_3`: STRUCT<
                `high_res`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `bitrate`: BIGINT,
                    `framerate`: BIGINT,
                    `codec`: INT,
                    `resolution`: INT,
                    `bitrate_control`: INT,
                    `idr_interval_seconds`: INT,
                    `video_transform`: STRUCT<
                      `flip_vertical`: BOOLEAN,
                      `flip_horizontal`: BOOLEAN,
                      `rotation`: INT
                    >,
                    `bframes_m_value`: INT
                  >
                >,
                `low_res`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `bitrate`: BIGINT,
                    `framerate`: BIGINT,
                    `codec`: INT,
                    `resolution`: INT,
                    `bitrate_control`: INT,
                    `idr_interval_seconds`: INT,
                    `video_transform`: STRUCT<
                      `flip_vertical`: BOOLEAN,
                      `flip_horizontal`: BOOLEAN,
                      `rotation`: INT
                    >,
                    `bframes_m_value`: INT
                  >
                >,
                `audio`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `samplerate`: BIGINT,
                    `audio_codec`: INT
                  >
                >
              >,
              `analog_4`: STRUCT<
                `high_res`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `bitrate`: BIGINT,
                    `framerate`: BIGINT,
                    `codec`: INT,
                    `resolution`: INT,
                    `bitrate_control`: INT,
                    `idr_interval_seconds`: INT,
                    `video_transform`: STRUCT<
                      `flip_vertical`: BOOLEAN,
                      `flip_horizontal`: BOOLEAN,
                      `rotation`: INT
                    >,
                    `bframes_m_value`: INT
                  >
                >,
                `low_res`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `bitrate`: BIGINT,
                    `framerate`: BIGINT,
                    `codec`: INT,
                    `resolution`: INT,
                    `bitrate_control`: INT,
                    `idr_interval_seconds`: INT,
                    `video_transform`: STRUCT<
                      `flip_vertical`: BOOLEAN,
                      `flip_horizontal`: BOOLEAN,
                      `rotation`: INT
                    >,
                    `bframes_m_value`: INT
                  >
                >,
                `audio`: STRUCT<
                  `mode`: INT,
                  `info`: STRUCT<
                    `samplerate`: BIGINT,
                    `audio_codec`: INT
                  >
                >
              >
            >,
            `excluded_safety_events`: ARRAY<INT>,
            `geofence_ids`: ARRAY<DECIMAL(20, 0)>,
            `geofence_coordinate_counts`: ARRAY<BIGINT>,
            `latitudes_microdegrees`: ARRAY<INT>,
            `longitudes_microdegrees`: ARRAY<INT>,
            `radii_meters_for_single_point_geofences`: ARRAY<BIGINT>
          >
        >,
        `gps_timeout_ms`: BIGINT,
        `gps_timeout_action`: INT
      >,
      `display`: STRUCT<
        `video_format`: INT,
        `default_views`: ARRAY<
          STRUCT<`media_input`: INT>
        >,
        `prioritized_trigger_rules`: ARRAY<
          STRUCT<
            `triggers`: ARRAY<INT>,
            `views`: ARRAY<
              STRUCT<`media_input`: INT>
            >
          >
        >
      >,
      `video_tokenization_mlapp_config`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `camera_inputs`: ARRAY<INT>,
        `dashcam_metadata_logging_enabled`: INT,
        `objectstat_logging_enabled`: INT,
        `objectstat_logging_interval_secs`: BIGINT,
        `model_fps_num`: BIGINT,
        `model_fps_den`: BIGINT
      >,
      `dashcam_vulnerable_road_user_collision_warning_mlapp_service_b_config`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `multi_tracker_config`: STRUCT<
          `distance_threshold`: FLOAT,
          `distance_fn_weights`: STRUCT<
            `iou_weight`: FLOAT,
            `cosine_weight`: FLOAT
          >,
          `ms_ahead`: BIGINT,
          `max_age_ms`: BIGINT
        >,
        `model_output_config`: STRUCT<
          `bbox_width_norm_factor`: FLOAT,
          `bbox_height_norm_factor`: FLOAT,
          `enable_nms`: BOOLEAN,
          `nms`: STRUCT<
            `max_iou_threshold`: FLOAT,
            `top_k`: INT,
            `max_count`: INT
          >,
          `filter`: STRUCT<
            `min_score_threshold`: FLOAT,
            `filter_label_ids`: ARRAY<INT>,
            `filter_tensor_names`: ARRAY<STRING>
          >
        >,
        `model_fps`: INT,
        `min_speed_kmph`: INT,
        `initialization_seconds`: FLOAT,
        `vehicle_line`: ARRAY<FLOAT>,
        `ttc_position_duration_seconds`: FLOAT,
        `ttc_distance_duration_seconds`: FLOAT,
        `harsh_events_debounce_ms`: BIGINT,
        `audio_alerts_debounce_ms`: BIGINT,
        `audio_alerts_enabled`: INT,
        `harsh_events_enabled`: INT,
        `gst_motion_estimation_config`: STRUCT<
          `momentum`: FLOAT,
          `max_estimation_gap_ms`: BIGINT
        >,
        `ego_motion_threshold`: FLOAT,
        `roi_scaling_factor`: FLOAT,
        `severity_score_threshold`: FLOAT,
        `enable_trajectory_severity_score`: BOOLEAN,
        `depth_reward_weight`: FLOAT,
        `depth_penalty_weight`: FLOAT,
        `object_approaching_severity_score`: FLOAT,
        `object_leaving_severity_score`: FLOAT,
        `object_staying_severity_score`: FLOAT,
        `disable_low_severity_events`: BOOLEAN,
        `low_severity_score_threshold`: FLOAT,
        `roi_corners`: ARRAY<
          STRUCT<
            `x`: FLOAT,
            `y`: FLOAT
          >
        >
      >,
      `manager_health`: STRUCT<`enabled`: INT>,
      `hi_res_stills`: STRUCT<
        `enabled`: INT,
        `capture_interval_secs`: BIGINT,
        `quality`: BIGINT,
        `enabled_harsh_event_types`: ARRAY<INT>,
        `report_start_offset_ms`: BIGINT,
        `report_duration_ms`: BIGINT
      >,
      `in_cab_voice_command`: STRUCT<
        `enabled`: INT,
        `command_confidence_threshold`: FLOAT,
        `wake_word_confidence_threshold`: FLOAT,
        `listen_for_command_led`: STRUCT<
          `led_display_enabled`: INT,
          `led_red_value`: BIGINT,
          `led_green_value`: BIGINT,
          `led_blue_value`: BIGINT
        >,
        `noise_suppression_enabled`: INT,
        `global_cooldown_ms`: DECIMAL(20, 0),
        `wake_word_trigger_level`: BIGINT,
        `wake_word_refractory_period`: BIGINT,
        `wake_word_duplicate_timeout_ms`: DECIMAL(20, 0),
        `command_trigger_level`: BIGINT,
        `command_refractory_period`: BIGINT,
        `command_duplicate_timeout_ms`: DECIMAL(20, 0),
        `command_timeout_ms`: DECIMAL(20, 0),
        `command_cooldown_ms`: DECIMAL(20, 0),
        `wake_word_confidence_monitoring_threshold`: FLOAT,
        `command_confidence_monitoring_threshold`: FLOAT,
        `pre_amp_gain_db`: FLOAT,
        `post_amp_gain_db`: FLOAT,
        `noise_suppression_level_db`: FLOAT
      >,
      `id_badge_detection_mlapp`: STRUCT<
        `mlapp_config`: STRUCT<
          `run_control_config`: STRUCT<
            `run_always`: INT,
            `run_while_on_trip`: INT,
            `run_at_start_of_trip`: INT,
            `run_at_start_of_trip_duration_ms`: BIGINT,
            `run_period`: INT
          >,
          `model_config`: STRUCT<
            `dlcs`: ARRAY<
              STRUCT<
                `name`: STRING,
                `version`: STRING,
                `url`: STRING,
                `size`: BIGINT,
                `sha256`: BINARY,
                `preferred_runtime`: INT
              >
            >,
            `model_registry_key`: STRING
          >,
          `shadow_mode_config`: STRUCT<
            `audio_alerts_enabled`: INT,
            `shadow_events_enabled`: INT
          >,
          `fastcv_config`: STRUCT<`use_dsp`: INT>,
          `thermal_response_config`: STRUCT<
            `enabled`: INT,
            `temp_ranges`: ARRAY<
              STRUCT<
                `tsens`: INT,
                `t_min_mc`: BIGINT,
                `t_max_mc`: BIGINT,
                `num_buckets`: DECIMAL(20, 0),
                `min_fps_scale_factor`: STRUCT<
                  `numerator`: DECIMAL(20, 0),
                  `denominator`: DECIMAL(20, 0)
                >
              >
            >,
            `response_type`: INT,
            `interval_secs`: DECIMAL(20, 0)
          >,
          `app_version`: STRUCT<`major_version`: BIGINT>,
          `pipeline_run_config`: STRUCT<`runtime`: INT>,
          `post_processing_pipeline`: ARRAY<
            STRUCT<
              `name`: STRING,
              `all`: STRUCT<`dummy`: BIGINT>,
              `at_most_one`: STRUCT<`dummy`: BIGINT>,
              `source`: STRUCT<
                `source`: STRUCT<
                  `transformed_source`: STRUCT<
                    `transformations`: ARRAY<
                      STRUCT<
                        `vehicle_speed_filter`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `threshold_kmph`: BIGINT
                        >,
                        `m_of_n_temporal_detector`: STRUCT<
                          `window_size`: BIGINT,
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `score_threshold`: FLOAT
                        >,
                        `slice`: STRUCT<
                          `slices`: ARRAY<
                            STRUCT<
                              `start_index`: BIGINT,
                              `end_index`: BIGINT,
                              `step`: BIGINT
                            >
                          >
                        >,
                        `bounding_box_filter`: STRUCT<
                          `valid_region_min_x_frac`: FLOAT,
                          `valid_region_min_y_frac`: FLOAT,
                          `valid_region_max_x_frac`: FLOAT,
                          `valid_region_max_y_frac`: FLOAT,
                          `box_center_x_tensor_index`: ARRAY<BIGINT>,
                          `box_center_y_tensor_index`: ARRAY<BIGINT>,
                          `score_tensor_index`: ARRAY<BIGINT>
                        >,
                        `bounding_box_corners_to_center`: STRUCT<
                          `top_left_x_tensor_index`: ARRAY<BIGINT>,
                          `top_left_y_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                          `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                        >,
                        `width_to_proximity`: STRUCT<
                          `score_tensor_index`: ARRAY<BIGINT>,
                          `min_score_threshold`: FLOAT,
                          `width_tensor_index`: ARRAY<BIGINT>,
                          `class_tensor_index`: ARRAY<BIGINT>,
                          `proximity_output_tensor_index`: ARRAY<BIGINT>,
                          `class_widths`: ARRAY<
                            STRUCT<
                              `class_id`: BIGINT,
                              `width`: FLOAT
                            >
                          >
                        >,
                        `vehicle_speed_scale`: STRUCT<
                          `tensor_index`: ARRAY<BIGINT>,
                          `units`: INT
                        >
                      >
                    >
                  >,
                  `tensor_name`: STRING
                >,
                `sinks`: ARRAY<
                  STRUCT<
                    `transformed_sink`: STRUCT<
                      `transformations`: ARRAY<
                        STRUCT<
                          `vehicle_speed_filter`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `threshold_kmph`: BIGINT
                          >,
                          `m_of_n_temporal_detector`: STRUCT<
                            `window_size`: BIGINT,
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `score_threshold`: FLOAT
                          >,
                          `slice`: STRUCT<
                            `slices`: ARRAY<
                              STRUCT<
                                `start_index`: BIGINT,
                                `end_index`: BIGINT,
                                `step`: BIGINT
                              >
                            >
                          >,
                          `bounding_box_filter`: STRUCT<
                            `valid_region_min_x_frac`: FLOAT,
                            `valid_region_min_y_frac`: FLOAT,
                            `valid_region_max_x_frac`: FLOAT,
                            `valid_region_max_y_frac`: FLOAT,
                            `box_center_x_tensor_index`: ARRAY<BIGINT>,
                            `box_center_y_tensor_index`: ARRAY<BIGINT>,
                            `score_tensor_index`: ARRAY<BIGINT>
                          >,
                          `bounding_box_corners_to_center`: STRUCT<
                            `top_left_x_tensor_index`: ARRAY<BIGINT>,
                            `top_left_y_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_x_tensor_index`: ARRAY<BIGINT>,
                            `bottom_right_y_tensor_index`: ARRAY<BIGINT>
                          >,
                          `width_to_proximity`: STRUCT<
                            `score_tensor_index`: ARRAY<BIGINT>,
                            `min_score_threshold`: FLOAT,
                            `width_tensor_index`: ARRAY<BIGINT>,
                            `class_tensor_index`: ARRAY<BIGINT>,
                            `proximity_output_tensor_index`: ARRAY<BIGINT>,
                            `class_widths`: ARRAY<
                              STRUCT<
                                `class_id`: BIGINT,
                                `width`: FLOAT
                              >
                            >
                          >,
                          `vehicle_speed_scale`: STRUCT<
                            `tensor_index`: ARRAY<BIGINT>,
                            `units`: INT
                          >
                        >
                      >
                    >,
                    `print`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `prefix`: STRING
                    >,
                    `harsh_event`: STRUCT<
                      `score_tensor_index`: ARRAY<BIGINT>,
                      `threshold`: FLOAT,
                      `harsh_accel_type`: BIGINT,
                      `detection_metadata_mode`: BIGINT,
                      `min_event_period_samples`: BIGINT
                    >
                  >
                >
              >
            >
          >
        >,
        `model_fps`: BIGINT,
        `dashcam_metadata_logging_enabled`: INT,
        `debug_objectstat_logging_enabled`: INT,
        `debug_objectstat_logging_interval_ms`: BIGINT,
        `debug_objectstat_include_image_enabled`: INT,
        `minimum_number_badge_features_for_detection`: BIGINT,
        `barcode_mandatory_enabled`: INT,
        `visible_led_illumination_duration_ms`: BIGINT,
        `rgb_led_illumination_color`: STRUCT<
          `red`: BIGINT,
          `green`: BIGINT,
          `blue`: BIGINT
        >
      >,
      `ai_two_way_agent`: STRUCT<
        `enabled`: INT,
        `echo_handling_method`: INT,
        `rms_mute_method`: STRUCT<
          `silence_threshold`: BIGINT,
          `capture_duration_ms`: BIGINT
        >,
        `default_happy_robot_config`: STRUCT<
          `api_host`: STRING,
          `org_id`: STRING,
          `api_key`: STRING,
          `use_case_id`: STRING,
          `json_params`: STRING
        >,
        `audio_output`: STRUCT<
          `speaker_output_percentage`: BIGINT,
          `pcm_amplitude_modifier`: FLOAT
        >
      >,
      `location_streaming`: STRUCT<
        `enabled`: INT,
        `min_interval_seconds`: BIGINT,
        `stream_uuids`: ARRAY<BINARY>,
        `max_backlog_batch_size`: BIGINT
      >
    >,
    `firmware_build`: STRING
  >
>,
`_synced_at` TIMESTAMP
