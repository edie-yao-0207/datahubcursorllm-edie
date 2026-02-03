`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`workflow_id` STRING,
`object_ids` STRING,
`created_at_ms` BIGINT,
`occurred_at_ms` BIGINT,
`proto` STRUCT<
  `trigger_matches`: ARRAY<
    STRUCT<
      `trigger_type`: BIGINT,
      `occurred_at_ms`: BIGINT,
      `object_type`: INT,
      `object_id`: BIGINT,
      `object_identifier`: STRING,
      `trigger_config_proto`: STRUCT<
        `measure`: STRUCT<
          `numeric`: STRUCT<
            `operation`: INT,
            `threshold`: DOUBLE,
            `unit`: BIGINT,
            `upper_threshold`: DOUBLE
          >,
          `min_activity_duration_ms`: BIGINT,
          `hos_violation`: STRUCT<
            `hos_alert_type`: INT,
            `ms_in_advance`: DECIMAL(20, 0)
          >,
          `eld_malfunction_or_diagnostic`: STRUCT<
            `malfunction_or_diagnostic_code`: STRING,
            `is_driver_object_type`: BOOLEAN
          >,
          `tire_fault_codes`: STRUCT<
            `all_critical`: BOOLEAN,
            `all_noncritical`: BOOLEAN,
            `tire_fault_codes`: ARRAY<INT>,
            `manufacturer`: INT
          >,
          `temperature`: STRUCT<
            `use_door_closed_condition`: BOOLEAN,
            `use_cargo_full_condition`: BOOLEAN
          >,
          `reefer_alarm`: STRUCT<`ignore_passive_alarms`: BOOLEAN>,
          `reefer_temperature`: STRUCT<
            `temp_delta_diff_threshold_milli_c`: DOUBLE,
            `operation`: INT,
            `zone`: BIGINT
          >,
          `vehicle_trailer_mismatch_configuration`: STRUCT<`selected_condition`: INT>,
          `trailer_moving_without_power_configuration`: STRUCT<`speed`: DOUBLE>,
          `charge_status_changed`: STRUCT<
            `statuses`: STRUCT<
              `complete`: BOOLEAN,
              `missed_target_charge`: BOOLEAN,
              `expected_to_miss_target_charge`: BOOLEAN,
              `charging`: BOOLEAN,
              `low`: BOOLEAN,
              `late_for_scheduled_charge`: BOOLEAN
            >
          >,
          `idling_alert_config`: STRUCT<
            `air_temp_filter`: STRUCT<
              `air_temp_range`: STRUCT<
                `min_temp_inclusive_c`: INT,
                `max_temp_inclusive_c`: INT
              >,
              `only_trigger_if_air_temp_available`: BOOLEAN
            >,
            `use_pto_filter`: BOOLEAN
          >,
          `wheel_end_fault_codes`: STRUCT<
            `wheel_end_fault_codes`: ARRAY<INT>,
            `manufacturer`: INT
          >,
          `hos_duty_status`: STRUCT<
            `duty_statuses`: ARRAY<INT>,
            `duration_ms`: BIGINT,
            `drivers`: STRUCT<
              `tag_ids`: ARRAY<BIGINT>,
              `driver_ids`: ARRAY<BIGINT>
            >
          >,
          `qualification_missing_or_expired`: STRUCT<
            `entity_type`: INT,
            `qualification_type_uuids`: ARRAY<BINARY>,
            `statuses`: ARRAY<INT>
          >
        >,
        `harsh_event`: STRUCT<
          `harsh_event_types`: ARRAY<INT>
        >,
        `dvir_submitted`: STRUCT<
          `dvir_submitted_types`: ARRAY<INT>,
          `min_duration_ms`: BIGINT,
          `defects`: STRUCT<
            `filter`: INT,
            `type_uuids`: ARRAY<BINARY>
          >
        >,
        `document_submitted`: STRUCT<
          `template_uuids`: ARRAY<BINARY>
        >,
        `scheduled_maintenance`: STRUCT<
          `schedule_id`: BIGINT,
          `ms_in_advance`: BIGINT
        >,
        `scheduled_maintenance_odometer`: STRUCT<
          `schedule_id`: BIGINT,
          `meters_in_advance`: BIGINT
        >,
        `scheduled_maintenance_engine_hours`: STRUCT<
          `schedule_id`: BIGINT,
          `engine_ms_in_advance`: BIGINT
        >,
        `geofence`: STRUCT<
          `geofence_circle`: STRUCT<
            `name`: STRING,
            `radius_meters`: DOUBLE,
            `center_lat`: DOUBLE,
            `center_lng`: DOUBLE
          >,
          `geofence_polygon`: STRUCT<
            `name`: STRING,
            `vertices`: ARRAY<
              STRUCT<
                `lat`: DOUBLE,
                `lng`: DOUBLE
              >
            >,
            `bounding_box`: STRUCT<
              `lat_max`: DOUBLE,
              `lng_max`: DOUBLE,
              `lat_min`: DOUBLE,
              `lng_min`: DOUBLE
            >
          >,
          `address_ids`: ARRAY<BIGINT>,
          `tag_ids`: ARRAY<BIGINT>,
          `address_types`: ARRAY<BIGINT>
        >,
        `out_of_route`: STRUCT<`distance_threshold_meters`: DECIMAL(20, 0)>,
        `route_stop_eta`: STRUCT<
          `alert_before_arrival_ms`: BIGINT,
          `include_live_share_link`: BOOLEAN,
          `alert_en_route_stops_only`: BOOLEAN
        >,
        `prioritized_vehicle_fault_codes`: STRUCT<
          `vehicle_fault_codes`: ARRAY<
            STRUCT<
              `fault_code`: STRING,
              `type`: INT
            >
          >,
          `red_lamp`: BOOLEAN,
          `mil`: BOOLEAN,
          `amber_lamp`: BOOLEAN,
          `protection_lamp`: BOOLEAN,
          `target_all_codes`: BOOLEAN,
          `trailer_abs_lamp`: BOOLEAN,
          `tts_lamp_red`: BOOLEAN,
          `tts_lamp_yellow`: BOOLEAN,
          `tts_triggers`: ARRAY<
            STRUCT<
              `indicator`: INT,
              `condition`: INT
            >
          >,
          `ebs_lamp_red`: BOOLEAN,
          `ebs_lamp_amber`: BOOLEAN
        >,
        `panic_button`: STRUCT<`filter_out_power_loss`: BOOLEAN>,
        `sites_camera_stream_areas_of_interest`: STRUCT<
          `uuids`: ARRAY<BINARY>
        >,
        `eld_malfunction_or_diagnostic`: STRUCT<
          `malfunction_or_diagnostic_code`: STRING,
          `is_driver_object_type`: BOOLEAN
        >,
        `form_submitted`: STRUCT<
          `form_template_uuids`: ARRAY<BINARY>,
          `form_score_threshold`: STRUCT<
            `operation`: INT,
            `threshold`: DOUBLE,
            `unit`: BIGINT,
            `upper_threshold`: DOUBLE
          >,
          `field_definitions_filter`: ARRAY<
            STRUCT<
              `single_select_field_definition_filter`: STRUCT<
                `field_definition_uuid`: BINARY,
                `selected_option_uuid`: BINARY
              >,
              `multi_select_field_definition_filter`: STRUCT<
                `field_definition_uuid`: BINARY,
                `selected_option_uuids`: ARRAY<BINARY>
              >
            >
          >
        >,
        `predicted_maintenance_issues`: STRUCT<
          `predicted_issues_types`: ARRAY<INT>
        >,
        `schedule`: STRUCT<
          `schedule_recurrence_type`: INT,
          `timezone`: STRING,
          `start_time_ms`: BIGINT,
          `custom_schedule`: STRUCT<
            `interval_weeks`: BIGINT,
            `days_of_week`: ARRAY<INT>
          >,
          `end_time_ms`: BIGINT
        >,
        `issue_created`: STRUCT<
          `form_template_uuids`: ARRAY<BINARY>,
          `assets`: STRUCT<
            `tag_ids`: ARRAY<BIGINT>,
            `asset_ids`: ARRAY<BIGINT>
          >
        >,
        `training_assignment_near_due_date`: STRUCT<
          `assignment_groups`: ARRAY<
            STRUCT<
              `type`: INT,
              `uuid`: BINARY
            >
          >,
          `condition_units`: INT,
          `condition_value`: BIGINT,
          `timezone`: STRING
        >,
        `worker_safety_sos_emitted`: STRUCT<
          `sources`: ARRAY<INT>,
          `alert_on_all_sources`: BOOLEAN
        >,
        `asset_tag_added`: STRUCT<
          `tag_ids`: ARRAY<BIGINT>
        >,
        `route_start_delayed`: STRUCT<
          `condition_units`: INT,
          `condition_value`: BIGINT,
          `timezone`: STRING
        >,
        `tag_driver_added`: STRUCT<
          `tag_ids`: ARRAY<BIGINT>
        >,
        `emr_reading`: STRUCT<
          `id`: STRING,
          `entity_id`: STRING,
          `type`: INT,
          `enum_value`: STRUCT<
            `symbol`: STRING,
            `number`: BIGINT
          >
        >,
        `safety_behavior`: STRUCT<
          `behaviors`: ARRAY<INT>,
          `statuses`: ARRAY<STRING>,
          `drivers`: STRUCT<
            `tag_ids`: ARRAY<BIGINT>,
            `driver_ids`: ARRAY<BIGINT>
          >,
          `safety_score`: STRUCT<
            `comparison`: INT,
            `score`: BIGINT
          >,
          `behavior_count`: STRUCT<
            `comparison`: INT,
            `num_behaviors`: BIGINT,
            `num_days`: BIGINT
          >,
          `severities`: ARRAY<INT>,
          `coached_event_count`: STRUCT<
            `comparison`: INT,
            `num_coached_events`: BIGINT,
            `num_days`: BIGINT
          >
        >,
        `fuel_level_anomaly`: STRUCT<`min_fuel_level_change_in_percents`: BIGINT>,
        `severe_weather_alert`: STRUCT<
          `severity`: INT,
          `severities`: ARRAY<INT>
        >,
        `health_status_alerting_config`: STRUCT<
          `products`: ARRAY<STRING>,
          `health_status_categories`: ARRAY<INT>,
          `health_status_codes`: ARRAY<INT>
        >,
        `form_updated`: STRUCT<
          `form_template_uuids`: ARRAY<BINARY>,
          `targets`: ARRAY<
            STRUCT<
              `type`: INT,
              `ids`: ARRAY<BIGINT>
            >
          >,
          `statuses`: ARRAY<STRING>
        >,
        `coaching_past_due`: STRUCT<
          `condition_units`: INT,
          `condition_value`: BIGINT,
          `coaching_type`: INT,
          `timezone`: STRING,
          `due_date_units`: INT,
          `due_date_value`: BIGINT
        >,
        `excluded_vehicle_fault_codes`: STRUCT<
          `vehicle_fault_codes`: ARRAY<
            STRUCT<
              `fault_code`: STRING,
              `type`: INT
            >
          >,
          `red_lamp`: BOOLEAN,
          `mil`: BOOLEAN,
          `amber_lamp`: BOOLEAN,
          `protection_lamp`: BOOLEAN,
          `target_all_codes`: BOOLEAN,
          `trailer_abs_lamp`: BOOLEAN,
          `tts_lamp_red`: BOOLEAN,
          `tts_lamp_yellow`: BOOLEAN,
          `tts_triggers`: ARRAY<
            STRUCT<
              `indicator`: INT,
              `condition`: INT
            >
          >,
          `ebs_lamp_red`: BOOLEAN,
          `ebs_lamp_amber`: BOOLEAN
        >,
        `qualifications_expired_or_expiring`: STRUCT<
          `qualification_type_uuids`: ARRAY<BINARY>,
          `condition_value`: BIGINT
        >,
        `immobilizer_state_changed`: STRUCT<
          `change_reasons`: ARRAY<INT>,
          `relay_ids`: ARRAY<INT>
        >,
        `hos_logs_updated`: STRUCT<
          `duty_statuses`: ARRAY<INT>,
          `duration_ms`: BIGINT,
          `drivers`: STRUCT<
            `tag_ids`: ARRAY<BIGINT>,
            `driver_ids`: ARRAY<BIGINT>
          >
        >,
        `script_execution`: STRUCT<
          `script_names`: ARRAY<STRING>,
          `statuses`: ARRAY<INT>
        >,
        `route_stop_early_late_arrival`: STRUCT<
          `alert_if_early_by_ms`: BIGINT,
          `alert_if_late_by_ms`: BIGINT,
          `include_live_share_link`: BOOLEAN,
          `alert_en_route_stops_only`: BOOLEAN
        >,
        `approaching_severe_weather`: STRUCT<
          `weather_types`: ARRAY<INT>
        >,
        `incident_created`: STRUCT<
          `incident_types`: ARRAY<INT>,
          `incident_priorities`: ARRAY<INT>,
          `incident_statuses`: ARRAY<INT>,
          `entity_types`: ARRAY<INT>
        >,
        `incident_updated`: STRUCT<
          `update_types`: ARRAY<INT>,
          `incident_types`: ARRAY<INT>,
          `incident_priorities`: ARRAY<INT>,
          `incident_statuses`: ARRAY<INT>,
          `resolution_statuses`: ARRAY<INT>,
          `entity_types`: ARRAY<INT>,
          `previous_incident_priorities`: ARRAY<INT>,
          `previous_incident_statuses`: ARRAY<INT>,
          `previous_resolution_statuses`: ARRAY<INT>
        >,
        `missing_dvir_past_due`: STRUCT<
          `dvir_form_template_uuids`: ARRAY<BINARY>
        >
      >,
      `match_details`: STRUCT<
        `dvir_created_by_driver`: STRUCT<
          `dvir_id`: DECIMAL(20, 0),
          `device_id`: DECIMAL(20, 0)
        >,
        `dvir_created_by_device`: STRUCT<
          `dvir_id`: BIGINT,
          `driver_id`: BIGINT
        >,
        `measure`: STRUCT<
          `numeric`: STRUCT<`value`: DOUBLE>
        >,
        `trigger_processing_metadata`: STRUCT<
          `geofence_metadata`: STRUCT<
            `name`: STRING,
            `address_id`: BIGINT
          >,
          `harsh_event`: STRUCT<
            `event_ms`: BIGINT,
            `event_id`: BIGINT,
            `harsh_accel_type`: INT
          >,
          `document_submitted`: STRUCT<
            `template_uuid`: BINARY,
            `driver_created_at_ms`: DECIMAL(20, 0)
          >,
          `route`: STRUCT<`dispatch_route_id`: BIGINT>,
          `route_stop_eta`: STRUCT<
            `dispatch_route_id`: BIGINT,
            `dispatch_job_id`: BIGINT,
            `estimated_arrival_at_ms`: BIGINT,
            `address_id`: BIGINT,
            `dispatch_job_destination_lat`: DOUBLE,
            `dispatch_job_destination_lng`: DOUBLE
          >,
          `vehicle_fault_data`: STRUCT<
            `j1939_data`: ARRAY<
              STRUCT<
                `tx_id`: INT,
                `mil_status`: INT,
                `red_lamp_status`: INT,
                `amber_lamp_status`: INT,
                `protect_lamp_status`: INT,
                `spn`: INT,
                `fmi`: INT,
                `occurance_count`: INT,
                `backend_only_j1939_fault_data`: STRUCT<
                  `fmi_description`: STRING,
                  `dtc_description`: STRING,
                  `volvo_repair_instructions_url`: STRING
                >,
                `fault_protocol_source`: INT,
                `deprecated_spn_version`: BOOLEAN
              >
            >,
            `passenger_data`: ARRAY<
              STRUCT<
                `tx_id`: INT,
                `mil_status`: BOOLEAN,
                `dtcs`: ARRAY<INT>,
                `pending_dtcs`: ARRAY<INT>,
                `permanent_dtcs`: ARRAY<INT>,
                `monitor_status`: ARRAY<
                  STRUCT<
                    `name`: INT,
                    `status`: INT
                  >
                >,
                `ignition_type`: INT,
                `mil_valid`: BOOLEAN,
                `dtcs_valid`: BOOLEAN,
                `pending_dtcs_valid`: BOOLEAN,
                `permanent_dtcs_valid`: BOOLEAN,
                `monitor_status_valid`: BOOLEAN,
                `ignition_type_valid`: BOOLEAN,
                `fault_protocol_source`: INT,
                `dtcs_with_severity_and_class`: ARRAY<
                  STRUCT<
                    `dtc`: INT,
                    `severity`: STRUCT<
                      `maintenance_only`: BOOLEAN,
                      `check_at_next_halt`: BOOLEAN,
                      `check_immediately`: BOOLEAN
                    >,
                    `class`: STRUCT<
                      `class_0`: BOOLEAN,
                      `class_1`: BOOLEAN,
                      `class_2`: BOOLEAN,
                      `class_3`: BOOLEAN,
                      `class_4`: BOOLEAN
                    >
                  >
                >,
                `pending_dtcs_with_severity_and_class`: ARRAY<
                  STRUCT<
                    `dtc`: INT,
                    `severity`: STRUCT<
                      `maintenance_only`: BOOLEAN,
                      `check_at_next_halt`: BOOLEAN,
                      `check_immediately`: BOOLEAN
                    >,
                    `class`: STRUCT<
                      `class_0`: BOOLEAN,
                      `class_1`: BOOLEAN,
                      `class_2`: BOOLEAN,
                      `class_3`: BOOLEAN,
                      `class_4`: BOOLEAN
                    >
                  >
                >
              >
            >,
            `oem_data`: ARRAY<
              STRUCT<
                `code_identifier`: STRING,
                `code_description`: STRING,
                `code_severity`: STRING,
                `code_source`: STRING
              >
            >,
            `j1587_data`: ARRAY<
              STRUCT<
                `tx_id`: INT,
                `j1587_fault_codes`: ARRAY<
                  STRUCT<
                    `id`: INT,
                    `is_pid`: BOOLEAN,
                    `fmi`: INT,
                    `occurance_count`: INT,
                    `active`: BOOLEAN
                  >
                >,
                `fault_codes_valid`: BOOLEAN,
                `trailer_abs_fault_lamp_status`: INT,
                `fault_protocol_source`: INT
              >
            >,
            `tts_data`: ARRAY<
              STRUCT<
                `indicator`: INT,
                `condition`: INT,
                `level`: INT,
                `change_count`: DECIMAL(20, 0)
              >
            >,
            `proprietary_protocol_data`: ARRAY<
              STRUCT<
                `manufacturer`: INT,
                `ecu_id`: BIGINT,
                `red_warning_lamp`: INT,
                `amber_warning_lamp`: INT,
                `time_since_last_rx_sec`: INT,
                `fault`: ARRAY<
                  STRUCT<
                    `fault_code_status`: INT,
                    `fault_code_type`: INT,
                    `fault_code`: STRUCT<
                      `dtc`: BIGINT,
                      `spn_fmi_fault`: STRUCT<
                        `spn`: BIGINT,
                        `fmi`: BIGINT
                      >
                    >
                  >
                >
              >
            >
          >,
          `dvir_submitted`: STRUCT<
            `duration_ms`: BIGINT,
            `dvir_type`: INT,
            `defect_labels`: ARRAY<STRING>,
            `defects`: ARRAY<
              STRUCT<
                `label`: STRING,
                `comment`: STRING,
                `id`: BIGINT
              >
            >,
            `dvir2_uuid`: BINARY,
            `safety_status`: INT,
            `dvir2_defects`: ARRAY<
              STRUCT<`uuid`: BINARY>
            >,
            `dvir_id`: BIGINT
          >,
          `hos_violation`: STRUCT<`violation_description`: STRING>,
          `sites_camera_stream_areas_of_interest`: STRUCT<
            `uuids`: ARRAY<BINARY>
          >,
          `speed`: STRUCT<`trip_start_ms`: BIGINT>,
          `tire_fault_metadata`: STRUCT<
            `tire_conditions`: ARRAY<
              STRUCT<
                `tire_from_left`: BIGINT,
                `axle_from_front`: BIGINT,
                `manufacturer`: INT,
                `tire_alerts`: ARRAY<INT>,
                `pressure_k_pa`: BIGINT,
                `temperature_milli_c`: INT,
                `tire_pressure_reference_info`: STRUCT<
                  `reference_pressure_k_pa`: BIGINT,
                  `reference_pressure_valid`: BOOLEAN
                >,
                `asset_type`: INT,
                `tire_alert_recommendations`: ARRAY<
                  STRUCT<
                    `alert_event`: STRING,
                    `recommendation_message`: STRING
                  >
                >
              >
            >
          >,
          `camera_connector_disconnected_metadata`: STRUCT<
            `disconnected_threshold_used`: BIGINT,
            `alert_type`: INT
          >,
          `sites_cloud_backup_issue_metadata`: STRUCT<
            `stream_ids`: ARRAY<BIGINT>
          >,
          `discrete_interval_end_event_metadata`: STRUCT<`start_ms`: BIGINT>,
          `severe_speeding_end_event_metadata`: STRUCT<`start_ms`: BIGINT>,
          `temperature_metadata`: STRUCT<
            `use_door_closed_condition`: BOOLEAN,
            `use_cargo_full_condition`: BOOLEAN,
            `door_widget_count`: BIGINT,
            `cargo_widget_count`: BIGINT
          >,
          `reefer_alarm_metadata`: STRUCT<
            `reefer_alarm`: STRUCT<
              `thermoking_codes`: ARRAY<BIGINT>,
              `carrier_codes`: ARRAY<BIGINT>,
              `carrier_eu_codes`: ARRAY<STRING>,
              `supra_alarm_state`: STRUCT<`led_on`: BOOLEAN>
            >
          >,
          `form_submitted`: STRUCT<
            `form_template_uuid`: BINARY,
            `form_submission_uuid`: BINARY,
            `form_score`: STRUCT<
              `form_score_points`: DOUBLE,
              `form_max_score_points`: BIGINT
            >
          >,
          `issue_created`: STRUCT<
            `asset_id`: BIGINT,
            `form_submission_uuid`: BINARY,
            `issue_subtype`: INT
          >,
          `reefer_temperature_metadata`: STRUCT<
            `critical_active_zones`: ARRAY<BIGINT>,
            `reported_temperatures`: ARRAY<INT>,
            `set_points`: ARRAY<INT>,
            `active_zones`: BIGINT
          >,
          `vehicle_trailer_mismatch_metadata`: STRUCT<
            `currently_pulling_trailer_ids`: ARRAY<BIGINT>,
            `driver_selected_trailer_ids`: ARRAY<BIGINT>
          >,
          `location_metadata`: STRUCT<
            `latitude`: DOUBLE,
            `longitude`: DOUBLE
          >,
          `predictive_maintenance_alert`: STRUCT<
            `predictive_maintenance_alert_uuid`: BINARY,
            `predictive_maintenance_type`: INT,
            `predictive_maintenance_alert_status`: INT,
            `predictive_maintenance_alert_severity`: INT
          >,
          `route_stop_resequence_metadata`: STRUCT<
            `before_state`: ARRAY<
              STRUCT<
                `id`: BIGINT,
                `job_status`: STRUCT<
                  `job_state`: INT,
                  `en_route_ms`: BIGINT,
                  `arrived_ms`: BIGINT,
                  `completed_ms`: BIGINT,
                  `skipped_ms`: BIGINT
                >,
                `time_window`: STRUCT<
                  `scheduled_arrival_time`: BIGINT,
                  `job_skip_threshold_ms`: BIGINT,
                  `scheduled_departure_time`: BIGINT
                >,
                `name`: STRING,
                `address`: STRING
              >
            >,
            `after_state`: ARRAY<
              STRUCT<
                `id`: BIGINT,
                `job_status`: STRUCT<
                  `job_state`: INT,
                  `en_route_ms`: BIGINT,
                  `arrived_ms`: BIGINT,
                  `completed_ms`: BIGINT,
                  `skipped_ms`: BIGINT
                >,
                `time_window`: STRUCT<
                  `scheduled_arrival_time`: BIGINT,
                  `job_skip_threshold_ms`: BIGINT,
                  `scheduled_departure_time`: BIGINT
                >,
                `name`: STRING,
                `address`: STRING
              >
            >,
            `notes`: STRING
          >,
          `trialer_moving_without_power_metadata`: STRUCT<`current_voltage`: INT>,
          `inaccurate_height_recorded_metadata`: STRUCT<
            `dvir_id`: BIGINT,
            `device_id`: BIGINT,
            `height_meter`: FLOAT
          >,
          `charge_status_changed_metadata`: STRUCT<
            `statuses`: STRUCT<
              `complete`: BOOLEAN,
              `missed_target_charge`: BOOLEAN,
              `expected_to_miss_target_charge`: BOOLEAN,
              `charging`: BOOLEAN,
              `low`: BOOLEAN,
              `late_for_scheduled_charge`: BOOLEAN
            >
          >,
          `low_bridge_detected_metadata`: STRUCT<
            `bridge_height_meters`: FLOAT,
            `vehicle_height_meters`: FLOAT,
            `bridge_distance_meters`: FLOAT
          >,
          `training_assignment_due_date_metadata`: ARRAY<
            STRUCT<`assigned_to_polymorphic`: STRING>
          >,
          `worker_safety_sos_signal_emitted_metadata`: STRUCT<`sos_signal_uuid`: BINARY>,
          `asset_tag_added_metadata`: STRUCT<`tag_id`: BIGINT>,
          `route_start_delayed_metadata`: STRUCT<`delay_ms`: BIGINT>,
          `tag_driver_added_metadata`: STRUCT<`tag_id`: BIGINT>,
          `wheel_end_metadata`: STRUCT<
            `wheel_ends`: ARRAY<
              STRUCT<
                `wheel_end_position`: STRUCT<
                  `axle_from_front`: BIGINT,
                  `wheel_end_side`: INT
                >,
                `wheel_end_reading`: STRUCT<
                  `temperature_milli_celsius`: INT,
                  `temperature_warning_active`: BOOLEAN,
                  `temperature_sensor_failure_active`: BOOLEAN,
                  `wheel_end_failure_status`: INT,
                  `sensor_battery_level_status`: INT,
                  `vibration_sensor_failure_status`: INT
                >
              >
            >,
            `manufacturer`: INT,
            `wheel_end_fault_codes`: ARRAY<INT>
          >,
          `out_of_sequence_stop_arrival_metadata`: STRUCT<
            `dispatch_route_id`: BIGINT,
            `actual_job_name`: STRING,
            `expected_job_name`: STRING
          >,
          `engine_idle_event`: STRUCT<
            `air_temperature_milli_c`: BIGINT,
            `pto_on`: BOOLEAN
          >,
          `safety_behavior_metadata`: STRUCT<
            `driver_id`: BIGINT,
            `behavior_label`: INT,
            `trigger_uuid`: BINARY,
            `safety_event_uuid`: BINARY,
            `uuid`: BINARY,
            `safety_score`: FLOAT,
            `behavior_counts`: ARRAY<
              STRUCT<
                `num_days`: BIGINT,
                `count`: BIGINT
              >
            >,
            `severity`: INT,
            `coached_event_counts`: ARRAY<
              STRUCT<
                `num_days`: BIGINT,
                `count`: BIGINT
              >
            >
          >,
          `fuel_level_anomaly_metadata`: STRUCT<
            `fuel_level_before_millipercent`: BIGINT,
            `fuel_level_after_millipercent`: BIGINT,
            `event_id`: STRING,
            `change_start_ms`: DECIMAL(20, 0),
            `change_end_ms`: DECIMAL(20, 0),
            `confidence_level_millipercent`: BIGINT,
            `device_id`: BIGINT,
            `org_id`: BIGINT
          >,
          `severe_weather_metadata`: STRUCT<
            `alert_enabled_affected_device_ids`: ARRAY<BIGINT>,
            `severe_weather_alert_id`: STRING
          >,
          `personal_conveyance_misuse_metadata`: STRUCT<
            `driver_id`: BIGINT,
            `device_id`: BIGINT,
            `event_start_ms`: BIGINT,
            `event_end_ms`: BIGINT,
            `shift_time_in_personal_conveyance_ms`: BIGINT,
            `shift_distance_in_personal_conveyance_meters`: BIGINT,
            `misuse_types`: ARRAY<INT>
          >,
          `form_updated`: STRUCT<
            `form_template_uuid`: BINARY,
            `form_submission_uuid`: BINARY,
            `updated_by_polymorphic_user_id`: STRING,
            `operation`: INT,
            `property_changes`: ARRAY<
              STRUCT<
                `property_name`: STRING,
                `old_value`: STRING,
                `new_value`: STRING
              >
            >
          >,
          `transmission_medium`: INT,
          `qualifications_missing_or_expired_metadata`: STRUCT<
            `qualification_types`: ARRAY<
              STRUCT<
                `uuid`: BINARY,
                `entity_type`: INT,
                `entity_id`: BIGINT,
                `title`: STRING,
                `status`: INT
              >
            >
          >,
          `coaching_past_due_date_metadata`: STRUCT<
            `driver_ids`: ARRAY<BIGINT>
          >,
          `qualifications_expired_or_expiring_metadata`: STRUCT<
            `entity_type`: INT,
            `status`: INT,
            `entity_ids`: ARRAY<BIGINT>,
            `expiry_date_start_ms`: BIGINT,
            `expiry_date_end_ms`: BIGINT,
            `expiry_date_formatted`: STRING
          >,
          `immobilizer_state_change_metadata`: STRUCT<
            `device_id`: BIGINT,
            `occurred_at_ms`: DECIMAL(20, 0),
            `relays`: ARRAY<
              STRUCT<
                `id`: BIGINT,
                `change_reason`: INT,
                `action`: INT
              >
            >
          >,
          `matching_health_status_metadata`: STRUCT<
            `matching_health_status`: ARRAY<
              STRUCT<
                `health_object`: INT,
                `rule_code`: INT,
                `status_code`: INT,
                `category`: INT
              >
            >
          >,
          `linked_asset_separation_metadata`: STRUCT<`last_associated_central_id`: BIGINT>,
          `driver_app_sign_in_metadata`: STRUCT<`pinned_vehicle_id`: BIGINT>,
          `hos_logs_updated_metadata`: STRUCT<
            `driver_id`: BIGINT,
            `duty_status`: BIGINT,
            `log_at_ms`: BIGINT
          >,
          `script_execution_metadata`: STRUCT<
            `correlation_id`: STRING,
            `status`: INT,
            `script_name`: STRING
          >,
          `route_stop_early_late_arrival`: STRUCT<
            `dispatch_route_id`: BIGINT,
            `dispatch_job_id`: BIGINT,
            `driver_id`: BIGINT,
            `scheduled_arrival_time_ms`: BIGINT,
            `actual_arrival_time_ms`: BIGINT,
            `ontime_window_before_arrival_ms`: BIGINT,
            `ontime_window_after_arrival_ms`: BIGINT,
            `address_id`: BIGINT,
            `dispatch_job_destination_lat`: DOUBLE,
            `dispatch_job_destination_lng`: DOUBLE
          >,
          `dvir2_submitted`: STRUCT<
            `dvir_form_submission_uuid`: BINARY,
            `duration_ms`: BIGINT,
            `safe_status`: INT,
            `defects`: ARRAY<
              STRUCT<`uuid`: BINARY>
            >
          >,
          `driver_assigned_to_vehicle_metadata`: STRUCT<
            `vehicle_id`: BIGINT,
            `assignment_source`: BIGINT
          >,
          `driver_unassigned_from_vehicle_metadata`: STRUCT<
            `vehicle_id`: BIGINT,
            `unassignment_source`: BIGINT
          >,
          `approaching_severe_weather_metadata`: STRUCT<
            `message_text`: STRING,
            `is_on_trip`: BOOLEAN,
            `weather_type`: INT
          >,
          `script_execution_action_context`: MAP<STRING, STRING>,
          `custom_detection_media_retrieval_metadata`: STRUCT<`request_id`: STRING>,
          `training_assignment_metadata`: STRUCT<
            `driver_ids`: ARRAY<BIGINT>,
            `user_ids`: ARRAY<BIGINT>
          >,
          `incident_center_incident_created_metadata`: STRUCT<
            `incident_id`: STRING,
            `incident_type`: INT,
            `incident_priority`: INT,
            `incident_status`: INT,
            `entity_type`: INT,
            `happened_at_ms`: DECIMAL(20, 0),
            `primary_entity_id`: STRING,
            `driver_id`: STRING,
            `user_id`: STRING,
            `location`: STRUCT<
              `latitude`: DOUBLE,
              `longitude`: DOUBLE,
              `formatted_address`: STRING
            >,
            `external_reference_id`: STRING,
            `details`: STRING
          >,
          `missing_dvir_past_due_metadata`: STRUCT<
            `dvir_submission_uuid`: BINARY,
            `due_date_ms`: BIGINT,
            `driver_id`: BIGINT
          >
        >
      >,
      `alertable_at_ms`: BIGINT,
      `is_secondary`: BOOLEAN,
      `resolved_at_ms`: BIGINT,
      `created_at_ms`: BIGINT,
      `platform_event`: STRUCT<
        `org_id`: BIGINT,
        `event_type`: BIGINT,
        `occurred_at_ms`: DECIMAL(20, 0),
        `created_at_ms`: DECIMAL(20, 0),
        `vehicle_created`: STRUCT<
          `vehicle_id`: BIGINT,
          `vehicle_name`: STRING,
          `vin`: STRING,
          `license_plate`: STRING,
          `gateway_serial`: STRING
        >,
        `dvir_submitted`: STRUCT<
          `dvir_id`: BIGINT,
          `device_id`: BIGINT,
          `driver_id`: BIGINT,
          `has_defects`: BOOLEAN,
          `defects_need_correction`: BOOLEAN,
          `duration_ms`: BIGINT,
          `defects`: ARRAY<
            STRUCT<
              `dvir_id`: BIGINT,
              `label`: STRING,
              `comment`: STRING,
              `type_uuid`: BINARY,
              `id`: BIGINT
            >
          >,
          `dvir2_uuid`: BINARY,
          `safety_status`: INT,
          `dvir2_defect_uuids`: ARRAY<BINARY>
        >,
        `driver_document_submitted`: STRUCT<
          `document_uuid`: BINARY,
          `driver_id`: BIGINT,
          `driver_created_at_ms`: DECIMAL(20, 0),
          `server_created_at_ms`: DECIMAL(20, 0),
          `server_updated_at_ms`: DECIMAL(20, 0),
          `document_type_uuid`: BINARY,
          `route_stop_id`: BIGINT,
          `document_name`: STRING
        >,
        `driver_message_received`: STRUCT<`driver_id`: BIGINT>,
        `driver_app_sign_in`: STRUCT<
          `driver_id`: BIGINT,
          `app_pinned_vehicle_id`: BIGINT,
          `app_pinned_vehicle_id_data_source`: BIGINT
        >,
        `driver_app_sign_out`: STRUCT<`driver_id`: BIGINT>,
        `route_stop_arrival`: STRUCT<
          `route_id`: BIGINT,
          `driver_id`: BIGINT,
          `device_id`: BIGINT,
          `dispatch_job_id`: BIGINT
        >,
        `route_stop_departure`: STRUCT<
          `route_id`: BIGINT,
          `driver_id`: BIGINT,
          `device_id`: BIGINT,
          `dispatch_job_id`: BIGINT
        >,
        `geofence_entry`: STRUCT<
          `device_id`: BIGINT,
          `address_id`: BIGINT
        >,
        `geofence_exit`: STRUCT<
          `device_id`: BIGINT,
          `address_id`: BIGINT
        >,
        `driver_created`: STRUCT<`driver_id`: BIGINT>,
        `address_created`: STRUCT<`address_id`: BIGINT>,
        `driver_updated`: STRUCT<`driver_id`: BIGINT>,
        `address_updated`: STRUCT<`address_id`: BIGINT>,
        `address_deleted`: STRUCT<
          `address_id`: BIGINT,
          `name`: STRING,
          `external_ids`: MAP<STRING, STRING>
        >,
        `vehicle_updated`: STRUCT<`vehicle_id`: BIGINT>,
        `engine_fault_on`: STRUCT<
          `details`: STRUCT<
            `device_id`: BIGINT,
            `fault_type`: INT,
            `j1939_fault`: STRUCT<
              `fault_code`: STRUCT<
                `spn`: INT,
                `fmi`: INT
              >,
              `lamp`: INT
            >,
            `passenger_fault`: STRUCT<
              `dtc`: INT,
              `lamp`: INT
            >,
            `oem_fault`: STRUCT<
              `code_identifier`: STRING,
              `code_description`: STRING,
              `code_severity`: STRING,
              `code_source`: STRING
            >
          >
        >,
        `engine_fault_off`: STRUCT<
          `details`: STRUCT<
            `device_id`: BIGINT,
            `fault_type`: INT,
            `j1939_fault`: STRUCT<
              `fault_code`: STRUCT<
                `spn`: INT,
                `fmi`: INT
              >,
              `lamp`: INT
            >,
            `passenger_fault`: STRUCT<
              `dtc`: INT,
              `lamp`: INT
            >,
            `oem_fault`: STRUCT<
              `code_identifier`: STRING,
              `code_description`: STRING,
              `code_severity`: STRING,
              `code_source`: STRING
            >
          >
        >,
        `gateway_unplugged`: STRUCT<`device_id`: BIGINT>,
        `gateway_plugged`: STRUCT<`device_id`: BIGINT>,
        `route_stop_estimated_arrival`: STRUCT<
          `dispatch_route_id`: BIGINT,
          `dispatch_job_id`: BIGINT,
          `device_id`: BIGINT,
          `estimated_arrival_at_ms`: BIGINT,
          `address_id`: BIGINT,
          `dispatch_job_destination_lat`: DOUBLE,
          `dispatch_job_destination_lng`: DOUBLE
        >,
        `driver_action_message_sent`: STRUCT<
          `message_uuid`: BINARY,
          `driver_id`: BIGINT
        >,
        `eld_malfunction_or_diagnostic_event_created`: STRUCT<
          `driver_id`: BIGINT,
          `device_id`: BIGINT,
          `malfunction_or_diagnostic_code`: STRING
        >,
        `end_to_end_started`: STRUCT<`device_id`: BIGINT>,
        `end_to_end_ended`: STRUCT<
          `device_id`: BIGINT,
          `started_at_ms`: BIGINT
        >,
        `severe_speeding_started`: STRUCT<
          `device_id`: BIGINT,
          `trip_start_ms`: BIGINT
        >,
        `severe_speeding_ended`: STRUCT<
          `device_id`: BIGINT,
          `started_at_ms`: BIGINT,
          `trip_start_ms`: BIGINT
        >,
        `alert_incident`: STRUCT<
          `happened_at_time`: BIGINT,
          `resolved_at_time`: BIGINT,
          `is_resolved`: BOOLEAN,
          `incident_url`: STRING,
          `updated_at_time`: BIGINT,
          `configuration_id`: STRING,
          `object_ids`: STRING,
          `conditions`: ARRAY<
            STRUCT<
              `trigger_id`: BIGINT,
              `description`: STRING,
              `is_primary`: BOOLEAN
            >
          >
        >,
        `form_submitted`: STRUCT<
          `form_submission_uuid`: BINARY,
          `form_template_uuid`: BINARY,
          `driver_id`: BIGINT,
          `form_score`: STRUCT<
            `form_score_points`: DOUBLE,
            `form_max_score_points`: BIGINT
          >,
          `form_product_type`: INT,
          `form_submission_duration_ms`: BIGINT,
          `asset_id`: BIGINT
        >,
        `issue_created`: STRUCT<
          `form_submission_uuid`: BINARY,
          `driver_id`: BIGINT,
          `asset_id`: BIGINT,
          `issue_subtype`: INT
        >,
        `alert_object_event`: STRUCT<
          `driver_id`: BIGINT,
          `vehicle_id`: BIGINT,
          `trailer_id`: BIGINT,
          `machine_input_id`: BIGINT,
          `onvif_camera_stream_id`: BIGINT,
          `workforce_camera_device_id`: BIGINT,
          `widget_id`: BIGINT
        >,
        `predictive_maintenance_alert`: STRUCT<
          `device_id`: BIGINT,
          `predictive_maintenance_alert_uuid`: BINARY,
          `predictive_maintenance_type`: INT,
          `predictive_maintenance_alert_status`: INT,
          `predictive_maintenance_alert_severity`: INT
        >,
        `route_stop_resequence`: STRUCT<
          `route_id`: BIGINT,
          `driver_id`: BIGINT,
          `device_id`: BIGINT,
          `before_state`: ARRAY<
            STRUCT<
              `id`: BIGINT,
              `job_status`: STRUCT<
                `job_state`: INT,
                `en_route_ms`: BIGINT,
                `arrived_ms`: BIGINT,
                `completed_ms`: BIGINT,
                `skipped_ms`: BIGINT
              >,
              `time_window`: STRUCT<
                `scheduled_arrival_time`: BIGINT,
                `job_skip_threshold_ms`: BIGINT,
                `scheduled_departure_time`: BIGINT
              >,
              `name`: STRING,
              `address`: STRING
            >
          >,
          `after_state`: ARRAY<
            STRUCT<
              `id`: BIGINT,
              `job_status`: STRUCT<
                `job_state`: INT,
                `en_route_ms`: BIGINT,
                `arrived_ms`: BIGINT,
                `completed_ms`: BIGINT,
                `skipped_ms`: BIGINT
              >,
              `time_window`: STRUCT<
                `scheduled_arrival_time`: BIGINT,
                `job_skip_threshold_ms`: BIGINT,
                `scheduled_departure_time`: BIGINT
              >,
              `name`: STRING,
              `address`: STRING
            >
          >,
          `notes`: STRING
        >,
        `driver_recorded`: STRUCT<
          `device_id`: BIGINT,
          `driver_id`: BIGINT,
          `trip_id`: BIGINT,
          `requested_at_ms`: BIGINT,
          `forward_video_url`: STRING,
          `inward_video_url`: STRING
        >,
        `workflows_platform_dummy`: STRUCT<`uuid`: BINARY>,
        `inaccurate_height_recorded`: STRUCT<
          `dvir_id`: BIGINT,
          `device_id`: BIGINT,
          `new_height`: FLOAT
        >,
        `worker_safety_sos_signal_emitted`: STRUCT<
          `sos_signal_uuid`: BINARY,
          `polymorphic_user_id`: STRING,
          `source_type`: INT
        >,
        `asset_tag_added`: STRUCT<
          `device_id`: BIGINT,
          `tag_id`: BIGINT
        >,
        `tag_driver_added`: STRUCT<
          `driver_id`: BIGINT,
          `tag_id`: BIGINT
        >,
        `out_of_sequence_stop_arrival`: STRUCT<
          `dispatch_route_id`: BIGINT,
          `actual_job_name`: STRING,
          `expected_job_name`: STRING,
          `device_id`: BIGINT
        >,
        `safety_behavior_alert`: STRUCT<
          `safety_behaviors`: ARRAY<
            STRUCT<
              `vehicle_id`: BIGINT,
              `event_ms`: BIGINT,
              `driver_id`: BIGINT,
              `behavior_label`: INT,
              `trigger_uuid`: BINARY,
              `safety_event_uuid`: BINARY,
              `uuid`: BINARY,
              `severity`: INT,
              `safety_score`: FLOAT,
              `behavior_counts`: ARRAY<
                STRUCT<
                  `num_days`: BIGINT,
                  `count`: BIGINT
                >
              >,
              `triage_severity`: INT,
              `trigger_type`: BIGINT,
              `event_severity`: INT,
              `coached_event_counts`: ARRAY<
                STRUCT<
                  `num_days`: BIGINT,
                  `count`: BIGINT
                >
              >
            >
          >
        >,
        `sudden_fuel_level_rise`: STRUCT<
          `event_id`: STRING,
          `change_start_ms`: DECIMAL(20, 0),
          `change_end_ms`: DECIMAL(20, 0),
          `fuel_level_before_millipercent`: BIGINT,
          `fuel_level_after_millipercent`: BIGINT,
          `confidence_level_millipercent`: BIGINT,
          `device_id`: BIGINT,
          `latitude`: DOUBLE,
          `longitude`: DOUBLE,
          `estimated_change_millipercent`: BIGINT,
          `estimated_change_low_millipercent`: BIGINT,
          `estimated_change_high_millipercent`: BIGINT
        >,
        `severe_weather_notification`: STRUCT<
          `alert_id`: STRING,
          `severity`: STRING,
          `event_type`: STRING,
          `affected_device_ids`: ARRAY<BIGINT>,
          `device_id`: BIGINT
        >,
        `sudden_fuel_level_drop`: STRUCT<
          `event_id`: STRING,
          `change_start_ms`: DECIMAL(20, 0),
          `change_end_ms`: DECIMAL(20, 0),
          `fuel_level_before_millipercent`: BIGINT,
          `fuel_level_after_millipercent`: BIGINT,
          `confidence_level_millipercent`: BIGINT,
          `device_id`: BIGINT,
          `latitude`: DOUBLE,
          `longitude`: DOUBLE
        >,
        `form_updated`: STRUCT<
          `form_submission_uuid`: BINARY,
          `operation`: INT,
          `property_changes`: ARRAY<
            STRUCT<
              `property_name`: STRING,
              `old_value`: STRING,
              `new_value`: STRING
            >
          >,
          `form_template_uuid`: BINARY,
          `submitted_by_polymorphic_user_id`: STRING
        >,
        `personal_conveyance_misuse`: STRUCT<
          `driver_id`: BIGINT,
          `device_id`: BIGINT,
          `event_start_ms`: BIGINT,
          `event_end_ms`: BIGINT,
          `shift_time_in_personal_conveyance_ms`: BIGINT,
          `shift_distance_in_personal_conveyance_meters`: BIGINT,
          `misuse_types`: ARRAY<INT>
        >,
        `route_stop_early_late_arrival`: STRUCT<
          `dispatch_route_id`: BIGINT,
          `dispatch_job_id`: BIGINT,
          `driver_id`: BIGINT,
          `device_id`: BIGINT,
          `scheduled_arrival_time_ms`: BIGINT,
          `actual_arrival_time_ms`: BIGINT,
          `ontime_window_before_arrival_ms`: BIGINT,
          `ontime_window_after_arrival_ms`: BIGINT,
          `address_id`: BIGINT,
          `dispatch_job_destination_lat`: DOUBLE,
          `dispatch_job_destination_lng`: DOUBLE
        >,
        `immobilizer_state_change`: STRUCT<
          `device_id`: BIGINT,
          `occurred_at_ms`: DECIMAL(20, 0),
          `relays`: ARRAY<
            STRUCT<
              `id`: BIGINT,
              `action`: INT,
              `change_reason`: INT
            >
          >
        >,
        `attribute_assigned`: STRUCT<
          `entity_type`: INT,
          `entity_id`: BIGINT,
          `attribute_type`: INT,
          `value_uuid`: STRING
        >,
        `attribute_unassigned`: STRUCT<
          `entity_type`: INT,
          `entity_id`: BIGINT,
          `attribute_type`: INT,
          `value_uuid`: STRING
        >,
        `attribute_deleted`: STRUCT<
          `entity_type`: INT,
          `attribute_type`: INT
        >,
        `settings_group_created`: STRUCT<`group_id`: STRING>,
        `settings_group_deleted`: STRUCT<`group_id`: STRING>,
        `settings_group_updated`: STRUCT<`group_id`: STRING>,
        `tag_deleted`: STRUCT<`tag_id`: BIGINT>,
        `hos_logs_updated`: STRUCT<
          `org_id`: BIGINT,
          `driver_id`: BIGINT,
          `computed_from_ms`: BIGINT,
          `computed_until_ms`: BIGINT,
          `published_at_ms`: BIGINT,
          `duty_status`: BIGINT,
          `log_at_ms`: BIGINT,
          `target_trigger_uuid`: STRING
        >,
        `driver_duty_status_changed`: STRUCT<
          `driver_id`: BIGINT,
          `duty_status`: BIGINT,
          `log_ms`: BIGINT
        >,
        `script_execution`: STRUCT<
          `script_name`: STRING,
          `status`: INT,
          `correlation_id`: STRING
        >,
        `dvir2_submitted`: STRUCT<
          `dvir_form_submission_uuid`: BINARY,
          `device_id`: BIGINT,
          `driver_id`: BIGINT,
          `duration_ms`: BIGINT,
          `safety_status`: INT,
          `defect_uuids`: ARRAY<BINARY>
        >,
        `suspicious_separation`: STRUCT<
          `correlation_uuid`: STRING,
          `device_id`: BIGINT,
          `anomaly_type`: STRING,
          `stat_correlation_time_ms`: BIGINT
        >,
        `driver_assigned_to_vehicle`: STRUCT<
          `driver_id`: BIGINT,
          `vehicle_id`: BIGINT,
          `assignment_source`: BIGINT
        >,
        `driver_unassigned_from_vehicle`: STRUCT<
          `driver_id`: BIGINT,
          `vehicle_id`: BIGINT,
          `unassignment_source`: BIGINT
        >,
        `speeding_event_started`: STRUCT<
          `device_id`: BIGINT,
          `trip_start_ms`: BIGINT,
          `severity_level`: INT
        >,
        `speeding_event_ended`: STRUCT<
          `device_id`: BIGINT,
          `started_at_ms`: BIGINT,
          `trip_start_ms`: BIGINT,
          `severity_level`: INT,
          `max_speed_over_kph`: FLOAT,
          `speed_limit_posted_kph`: FLOAT
        >,
        `approaching_severe_weather`: STRUCT<
          `device_id`: BIGINT,
          `radar`: STRUCT<
            `weather_type`: INT,
            `precipitation_type`: INT,
            `severity`: INT
          >,
          `public_alert`: STRUCT<
            `alert_id`: STRING,
            `nws_alert_type`: INT,
            `event_onset_at_ms`: BIGINT,
            `event_ends_at_ms`: BIGINT
          >
        >,
        `vehicle_weather_entry`: STRUCT<
          `device_id`: BIGINT,
          `precipitation_type`: INT
        >,
        `vehicle_weather_exit`: STRUCT<
          `device_id`: BIGINT,
          `precipitation_type`: INT
        >,
        `user_organization_added`: STRUCT<`user_id`: BIGINT>,
        `user_organization_removed`: STRUCT<`user_id`: BIGINT>,
        `user_updated`: STRUCT<`user_id`: BIGINT>,
        `incident_center_incident_created`: STRUCT<
          `incident_id`: STRING,
          `incident_type`: INT,
          `happened_at_ms`: DECIMAL(20, 0),
          `primary_entity_id`: STRING,
          `primary_entity_type`: INT,
          `driver_id`: STRUCT<`value`: BIGINT>,
          `user_id`: STRUCT<`value`: BIGINT>,
          `location`: STRUCT<
            `latitude`: DOUBLE,
            `longitude`: DOUBLE,
            `formatted_address`: STRING
          >,
          `external_reference_id`: STRING,
          `priority`: INT,
          `incident_status_v2`: INT,
          `details`: STRING
        >,
        `incident_center_incident_updated`: STRUCT<
          `incident_id`: STRING,
          `incident_type`: INT,
          `happened_at_ms`: DECIMAL(20, 0),
          `primary_entity_id`: STRING,
          `primary_entity_type`: INT,
          `driver_id`: STRUCT<`value`: BIGINT>,
          `user_id`: STRUCT<`value`: BIGINT>,
          `location`: STRUCT<
            `latitude`: DOUBLE,
            `longitude`: DOUBLE,
            `formatted_address`: STRING
          >,
          `external_reference_id`: STRING,
          `priority`: INT,
          `incident_status_v2`: INT,
          `resolution_status_v2`: STRUCT<`value`: INT>,
          `details`: STRING,
          `update_type`: INT,
          `previous_priority`: INT,
          `previous_incident_status_v2`: INT,
          `previous_resolution_status_v2`: STRUCT<`value`: INT>
        >,
        `missing_dvir_past_due`: STRUCT<
          `dvir_submission_uuid`: BINARY,
          `dvir_template_uuid`: BINARY,
          `due_date_ms`: BIGINT,
          `driver_id`: BIGINT
        >,
        `custom_detection_media_retrieval_completed`: STRUCT<
          `asset_id`: BIGINT,
          `request_id`: STRING
        >,
        `settings_changed`: STRUCT<
          `namespace`: STRING,
          `entity_type`: STRING,
          `entity_id`: STRING,
          `changes`: ARRAY<
            STRUCT<
              `path`: STRING,
              `old_value`: STRUCT<
                `bool_value`: BOOLEAN,
                `int_value`: BIGINT,
                `float_value`: DOUBLE,
                `string_value`: STRING,
                `string_list_value`: STRUCT<
                  `values`: ARRAY<STRING>
                >
              >,
              `new_value`: STRUCT<
                `bool_value`: BOOLEAN,
                `int_value`: BIGINT,
                `float_value`: DOUBLE,
                `string_value`: STRING,
                `string_list_value`: STRUCT<
                  `values`: ARRAY<STRING>
                >
              >
            >
          >,
          `user_id`: STRING
        >
      >,
      `updated_at_ms`: BIGINT
    >
  >
>,
`_raw_proto` STRING,
`updated_at_ms` BIGINT,
`resolved_at_ms` BIGINT,
`is_admin` BYTE,
`incident_status` STRING,
`resolution_processed_at_ms` BIGINT,
`name` STRING,
`description` STRING,
`action_status` STRING,
`severity` BYTE,
`date` STRING
