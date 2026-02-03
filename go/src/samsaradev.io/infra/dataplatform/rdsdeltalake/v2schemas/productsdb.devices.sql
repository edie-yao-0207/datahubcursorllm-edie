`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`id` BIGINT,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`lat` DOUBLE,
`lng` DOUBLE,
`name` STRING,
`group_id` BIGINT,
`config_override_json` STRING,
`public_key` STRING,
`auto_sleep_enabled` BYTE,
`product_id` BIGINT,
`obd_type` BIGINT,
`driver_id` BIGINT,
`chipset_serial` STRING,
`serial` STRING,
`order_id` BIGINT,
`samsara_note` STRING,
`user_note` STRING,
`digi1_type_id` BIGINT,
`digi2_type_id` BIGINT,
`ibeacon_major_minor` INT,
`manual_odometer_meters` BIGINT,
`manual_odometer_updated_at` TIMESTAMP,
`front_ultrasonic_widget_id` BIGINT,
`middle_ultrasonic_widget_id` BIGINT,
`back_ultrasonic_widget_id` BIGINT,
`vin` STRING,
`make` STRING,
`model` STRING,
`year` BIGINT,
`imei` STRING,
`iccid` STRING,
`camera_last_connected_at_ms` BIGINT,
`harsh_accel_setting` BIGINT,
`aobrd_mode_enabled` BYTE,
`device_color` STRING,
`camera_serial` STRING,
`camera_product_id` BIGINT,
`camera_first_connected_at_ms` BIGINT,
`manual_engine_hours` BIGINT,
`manual_engine_hours_updated_at` TIMESTAMP,
`is_ecm_vin` BYTE,
`quarantine_enabled` BYTE,
`device_settings_proto` STRUCT<
  `ethernet_setting`: STRUCT<
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
  `io_module_config`: STRUCT<
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
  `programs_config`: STRUCT<
    `plc_programs`: ARRAY<
      STRUCT<
        `program_id`: BIGINT,
        `program_hash`: STRING,
        `variables`: ARRAY<
          STRUCT<
            `name`: STRING,
            `machineInputId`: BIGINT,
            `machine_output_uuid`: STRING,
            `local_variable_id`: BIGINT,
            `gateway_io`: STRUCT<
              `pin_number`: BIGINT,
              `module_id`: BIGINT,
              `module_gateway_id`: BIGINT
            >,
            `type`: INT,
            `metadata`: STRUCT<
              `statType`: BIGINT,
              `objectType`: BIGINT,
              `objectId`: BIGINT,
              `formula`: STRING
            >,
            `variable_type`: BIGINT
          >
        >,
        `program_uuid`: STRING
      >
    >
  >,
  `audio_recording_device_enabled`: BOOLEAN,
  `mes_settings`: STRUCT<
    `mes_recipes`: ARRAY<
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
    `mes_work_orders`: ARRAY<
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
    `mes_lines`: ARRAY<
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
  `ig_tunnel_config`: STRUCT<
    `forwards`: ARRAY<
      STRUCT<
        `local_port`: BIGINT,
        `remote_gateway_id`: BIGINT,
        `remote_host`: STRING,
        `remote_port`: BIGINT
      >
    >
  >,
  `secondary_ethernet_setting`: STRUCT<
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
  `machine_vision_snapshot_config`: STRUCT<`snapshot_id`: BIGINT>,
  `turing_io_config`: STRUCT<
    `mod_config`: ARRAY<
      STRUCT<
        `slot_num`: BIGINT,
        `mod_type`: INT
      >
    >,
    `min_log_interval_ms`: BIGINT
  >,
  `slave_server_config`: STRUCT<
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
    `modbus_rtu_slave_id`: BIGINT,
    `serial_port`: INT
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
  `voice_coaching_device_enabled`: BOOLEAN,
  `forward_collision_warning_audio_alerts_device_enabled`: BOOLEAN,
  `distracted_driving_detection_audio_alerts_device_enabled`: BOOLEAN,
  `digi_inputs`: STRUCT<
    `inputs`: ARRAY<
      STRUCT<
        `port`: BIGINT,
        `input`: STRUCT<
          `input_type`: BIGINT,
          `registered_at`: BIGINT,
          `uuid`: STRING
        >
      >
    >
  >,
  `use_gps_speed_for_auto_duty`: BOOLEAN,
  `unregulated_vehicle`: BOOLEAN,
  `forward_collision_warning_roi_config`: STRUCT<
    `vehicle_center_diff_x_px`: INT,
    `horizon_line_percent`: FLOAT
  >,
  `oriented_harsh_event`: STRUCT<
    `harsh_accel_x_threshold_gs`: FLOAT,
    `harsh_brake_x_threshold_gs`: FLOAT,
    `harsh_turn_x_threshold_gs`: FLOAT
  >,
  `internal_low_power_mode_enabled`: BOOLEAN,
  `voice_coaching_speed_threshold_milliknots`: BIGINT,
  `device_audio_language`: INT,
  `voice_coaching_alert_config`: STRUCT<
    `seat_belt_unbuckled_alert_disabled`: BOOLEAN,
    `maximum_speed_alert_disabled`: BOOLEAN,
    `harsh_driving_alert_disabled`: BOOLEAN,
    `crash_alert_disabled`: BOOLEAN,
    `maximum_speed_time_before_alert_ms`: BIGINT
  >,
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
  `forward_collision_warning_safety_inbox_device_enabled`: BOOLEAN,
  `distracted_driving_detection_safety_inbox_device_enabled`: BOOLEAN,
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
      `upload_interval_ms`: BIGINT,
      `min_instance_interval_ms`: BIGINT,
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
  `following_distance_safety_inbox_device_enabled`: BOOLEAN,
  `following_distance_audio_alerts_device_enabled`: BOOLEAN,
  `camera_height_meters`: FLOAT,
  `safety`: STRUCT<
    `in_cab_speed_limit_alert_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `enable_audio_alerts`: BOOLEAN,
      `kmph_over_limit_enabled`: BOOLEAN,
      `kmph_over_limit_threshold`: FLOAT,
      `percent_over_limit_enabled`: BOOLEAN,
      `percent_over_limit_threshold`: FLOAT,
      `time_before_alert_ms`: BIGINT
    >,
    `recording`: STRUCT<`storage_hours_setting`: BIGINT>,
    `vanishing_point_estimation`: STRUCT<`calibration_version`: BIGINT>,
    `policy_violation_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `smoking_enabled`: BOOLEAN,
      `phone_usage_enabled`: BOOLEAN,
      `food_enabled`: BOOLEAN,
      `drink_enabled`: BOOLEAN,
      `seatbelt_enabled`: BOOLEAN,
      `mask_enabled`: BOOLEAN,
      `audio_alert_settings`: STRUCT<
        `enabled`: BOOLEAN,
        `smoking_enabled`: BOOLEAN,
        `phone_usage_enabled`: BOOLEAN,
        `food_enabled`: BOOLEAN,
        `drink_enabled`: BOOLEAN,
        `seatbelt_enabled`: BOOLEAN,
        `mask_enabled`: BOOLEAN,
        `camera_obstruction_enabled`: BOOLEAN,
        `inward_obstruction_enabled`: BOOLEAN,
        `outward_obstruction_enabled`: BOOLEAN
      >,
      `camera_obstruction_enabled`: BOOLEAN,
      `minimum_speed`: INT,
      `inward_obstruction_enabled`: BOOLEAN,
      `outward_obstruction_enabled`: BOOLEAN,
      `seatbelt_minimum_speed`: INT,
      `inward_obstruction_minimum_speed`: INT
    >,
    `in_cab_stop_sign_violation_alert_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `enable_audio_alerts`: BOOLEAN,
      `stop_threshold_kmph`: FLOAT
    >,
    `in_cab_railroad_crossing_violation_alert_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `enable_audio_alerts`: BOOLEAN,
      `stop_threshold_kmph`: FLOAT
    >,
    `livestream_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `outward_camera_enabled`: BOOLEAN,
      `inward_camera_enabled`: BOOLEAN,
      `audio_stream_enabled`: BOOLEAN
    >,
    `drowsiness_detection_settings`: STRUCT<
      `drowsiness_detection_enabled`: BOOLEAN,
      `drowsiness_detection_audio_alerts_enabled`: BOOLEAN,
      `drowsiness_detection_sensitivity_level`: INT
    >,
    `lane_departure_warning_settings`: STRUCT<
      `lane_departure_warning_enabled`: BOOLEAN,
      `lane_departure_warning_audio_alerts_enabled`: BOOLEAN
    >,
    `following_distance_settings`: STRUCT<
      `minimum_speed_enum`: INT,
      `minimum_duration_enum`: INT,
      `minimum_following_distance_seconds`: FLOAT
    >,
    `forward_collision_sensitivity_level`: INT,
    `distracted_driving_detection_settings`: STRUCT<
      `minimum_speed_enum`: INT,
      `minimum_duration_enum`: INT
    >
  >,
  `multicam`: STRUCT<
    `camera_info`: ARRAY<
      STRUCT<
        `stream_uuid`: STRING,
        `camera_rotation`: INT,
        `stream_id`: BIGINT
      >
    >,
    `camera_monitor_config`: STRUCT<
      `default_stream_uuid`: STRING,
      `default_stream_id`: BIGINT
    >
  >,
  `vulcan_module_config`: STRUCT<
    `module_id`: BIGINT,
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
    `minimum_log_interval`: BIGINT,
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
  >,
  `industrial`: STRUCT<`ig15_octopus_cable_with_no_peripheral`: BOOLEAN>,
  `vehicle`: STRUCT<
    `vehicle_type`: INT,
    `gross_vehicle_weight`: STRUCT<
      `weight`: BIGINT,
      `unit`: INT
    >
  >,
  `replay_mode`: STRUCT<
    `enabled`: BOOLEAN,
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
    `source_org_id`: BIGINT,
    `source_device_id`: BIGINT,
    `source_start_unix_ms`: BIGINT
  >,
  `device_sim_settings`: STRUCT<
    `sim_slot_failover_disabled`: INT,
    `sim_slot_failover_disabled_set`: BOOLEAN
  >,
  `syslog_retrieval`: STRUCT<`upload_syslogs_before_unix_ms`: BIGINT>,
  `plow_from_spreader`: STRUCT<`enabled`: BOOLEAN>,
  `dimensions`: STRUCT<`height_meters`: DOUBLE>,
  `efficiency_settings`: STRUCT<`idling_rpm`: BIGINT>,
  `device_remote_privacy_button_settings`: STRUCT<`remote_mode_enabled`: BOOLEAN>
>,
`_raw_device_settings_proto` STRING,
`sleep_override` BIGINT,
`mpg_city` BIGINT,
`mpg_hw` BIGINT,
`fuel_capacity` DOUBLE,
`is_multicam` BYTE,
`asset_serial` STRING,
`carrier_name_override` STRING,
`carrier_address_override` STRING,
`carrier_us_dot_number_override` BIGINT,
`proto` STRUCT<
  `LicensePlate`: STRING,
  `dynamic_product_features`: ARRAY<BIGINT>
>,
`_raw_proto` STRING,
`sleep_override_enabled` BYTE,
`cable_id` BIGINT,
`ignition_disabled` BYTE,
`asset_type` BIGINT,
`enabled_for_mobile` BYTE,
`order_uuid` STRING,
`variant_id` INT,
`org_id` BIGINT,
`camera_variant_id` INT,
`contains_internal_rma_required` BYTE,
`partition` STRING
