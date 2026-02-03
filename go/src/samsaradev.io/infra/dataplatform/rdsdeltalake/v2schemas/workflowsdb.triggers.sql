`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`uuid` STRING,
`org_id` BIGINT,
`workflow_uuid` STRING,
`type` INT,
`proto_data` STRUCT<
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
`_raw_proto_data` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`deleted_at` TIMESTAMP,
`deleted_by_user_id` BIGINT,
`is_secondary` BYTE,
`partition` STRING
