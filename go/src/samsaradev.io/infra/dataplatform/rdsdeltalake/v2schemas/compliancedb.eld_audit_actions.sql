`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`action_type` BYTE,
`vehicle_id` BIGINT,
`server_action_at` TIMESTAMP,
`driver_id` BIGINT,
`eld_event_start_at` TIMESTAMP,
`eld_event_end_at` TIMESTAMP,
`updated_by_user_id` BIGINT,
`client_action_at` TIMESTAMP,
`proto` STRUCT<
  `old_logs`: ARRAY<
    STRUCT<
      `id`: BIGINT,
      `org_id`: BIGINT,
      `group_id`: BIGINT,
      `vehicle_id`: BIGINT,
      `driver_id`: BIGINT,
      `log_ms`: BIGINT,
      `end_at`: BIGINT,
      `status_code`: BIGINT,
      `notes`: STRING,
      `loc_city`: STRING,
      `loc_state`: STRING,
      `loc_lat`: DOUBLE,
      `loc_lng`: DOUBLE,
      `loc_override`: STRING,
      `log_type`: BIGINT,
      `created_at`: BIGINT,
      `updated_at`: BIGINT,
      `active_to_inactive_changed_at_ms`: BIGINT,
      `inactive_change_requested_to_active_at_ms`: BIGINT,
      `inactive_change_requested_to_inactive_change_rejected_at_ms`: BIGINT,
      `event_record_status`: INT,
      `event_record_origin`: INT,
      `updated_by_user_id`: BIGINT,
      `status_code_override`: INT,
      `special_driving_start_log_id`: BIGINT,
      `special_driving_end_log_id`: BIGINT,
      `special_driving_child_end_log_id`: BIGINT
    >
  >,
  `new_logs`: ARRAY<
    STRUCT<
      `id`: BIGINT,
      `org_id`: BIGINT,
      `group_id`: BIGINT,
      `vehicle_id`: BIGINT,
      `driver_id`: BIGINT,
      `log_ms`: BIGINT,
      `end_at`: BIGINT,
      `status_code`: BIGINT,
      `notes`: STRING,
      `loc_city`: STRING,
      `loc_state`: STRING,
      `loc_lat`: DOUBLE,
      `loc_lng`: DOUBLE,
      `loc_override`: STRING,
      `log_type`: BIGINT,
      `created_at`: BIGINT,
      `updated_at`: BIGINT,
      `active_to_inactive_changed_at_ms`: BIGINT,
      `inactive_change_requested_to_active_at_ms`: BIGINT,
      `inactive_change_requested_to_inactive_change_rejected_at_ms`: BIGINT,
      `event_record_status`: INT,
      `event_record_origin`: INT,
      `updated_by_user_id`: BIGINT,
      `status_code_override`: INT,
      `special_driving_start_log_id`: BIGINT,
      `special_driving_end_log_id`: BIGINT,
      `special_driving_child_end_log_id`: BIGINT
    >
  >,
  `logs_created`: ARRAY<
    STRUCT<
      `id`: BIGINT,
      `org_id`: BIGINT,
      `group_id`: BIGINT,
      `vehicle_id`: BIGINT,
      `driver_id`: BIGINT,
      `log_ms`: BIGINT,
      `end_at`: BIGINT,
      `status_code`: BIGINT,
      `notes`: STRING,
      `loc_city`: STRING,
      `loc_state`: STRING,
      `loc_lat`: DOUBLE,
      `loc_lng`: DOUBLE,
      `loc_override`: STRING,
      `log_type`: BIGINT,
      `created_at`: BIGINT,
      `updated_at`: BIGINT,
      `active_to_inactive_changed_at_ms`: BIGINT,
      `inactive_change_requested_to_active_at_ms`: BIGINT,
      `inactive_change_requested_to_inactive_change_rejected_at_ms`: BIGINT,
      `event_record_status`: INT,
      `event_record_origin`: INT,
      `updated_by_user_id`: BIGINT,
      `status_code_override`: INT,
      `special_driving_start_log_id`: BIGINT,
      `special_driving_end_log_id`: BIGINT,
      `special_driving_child_end_log_id`: BIGINT
    >
  >,
  `logs_deleted`: ARRAY<
    STRUCT<
      `id`: BIGINT,
      `org_id`: BIGINT,
      `group_id`: BIGINT,
      `vehicle_id`: BIGINT,
      `driver_id`: BIGINT,
      `log_ms`: BIGINT,
      `end_at`: BIGINT,
      `status_code`: BIGINT,
      `notes`: STRING,
      `loc_city`: STRING,
      `loc_state`: STRING,
      `loc_lat`: DOUBLE,
      `loc_lng`: DOUBLE,
      `loc_override`: STRING,
      `log_type`: BIGINT,
      `created_at`: BIGINT,
      `updated_at`: BIGINT,
      `active_to_inactive_changed_at_ms`: BIGINT,
      `inactive_change_requested_to_active_at_ms`: BIGINT,
      `inactive_change_requested_to_inactive_change_rejected_at_ms`: BIGINT,
      `event_record_status`: INT,
      `event_record_origin`: INT,
      `updated_by_user_id`: BIGINT,
      `status_code_override`: INT,
      `special_driving_start_log_id`: BIGINT,
      `special_driving_end_log_id`: BIGINT,
      `special_driving_child_end_log_id`: BIGINT
    >
  >,
  `handle_driver_signout_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `mobile_app_id`: BIGINT,
    `duty_status_code`: BIGINT,
    `has_duty_status_code`: BOOLEAN,
    `vehicle_id`: BIGINT
  >,
  `sign_in_driver_to_vehicle_request`: STRUCT<
    `org_id`: BIGINT,
    `driver_id`: BIGINT,
    `vehicle_id`: BIGINT,
    `time_ms`: BIGINT,
    `session_uuid`: STRING,
    `force_sign_out_session_uuid`: STRING,
    `mobile_app_id`: BIGINT
  >,
  `sign_in_passenger_to_vehicle_request`: STRUCT<
    `org_id`: BIGINT,
    `driver_id`: BIGINT,
    `vehicle_id`: BIGINT,
    `time_ms`: BIGINT,
    `session_uuid`: STRING,
    `mobile_app_id`: BIGINT
  >,
  `sign_out_from_vehicle_request`: STRUCT<
    `org_id`: BIGINT,
    `driver_id`: BIGINT,
    `vehicle_id`: BIGINT,
    `time_ms`: BIGINT,
    `session_uuid`: STRING,
    `mobile_app_id`: BIGINT,
    `duty_status_changes`: ARRAY<
      STRUCT<
        `driver_id`: BIGINT,
        `duty_status`: INT
      >
    >
  >,
  `change_driver_request`: STRUCT<
    `org_id`: BIGINT,
    `vehicle_id`: BIGINT,
    `time_ms`: BIGINT,
    `mobile_app_id`: BIGINT,
    `new_driver_id`: BIGINT,
    `old_passenger_session_uuid`: STRING,
    `new_driver_session_uuid`: STRING,
    `old_driver_id`: BIGINT,
    `old_driver_session_uuid`: STRING,
    `new_passenger_session_uuid`: STRING
  >,
  `set_current_duty_status_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `duty_status`: INT,
    `created_at_ms`: BIGINT,
    `old_duty_status`: INT,
    `vehicle_id`: BIGINT,
    `annotation`: STRING,
    `manual_location`: STRING,
    `mobile_app_id`: BIGINT
  >,
  `go_off_duty_for_sign_out_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `created_at_ms`: BIGINT,
    `old_duty_status`: INT,
    `vehicle_id`: BIGINT,
    `mobile_app_id`: BIGINT
  >,
  `go_on_duty_for_dvir_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `created_at_ms`: BIGINT,
    `old_duty_status`: INT,
    `vehicle_id`: BIGINT,
    `mobile_app_id`: BIGINT
  >,
  `dismiss_auto_duty_still_driving_prompt_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `prompted_at_ms`: BIGINT,
    `new_duty_status`: INT,
    `created_at_ms`: BIGINT,
    `vehicle_id`: BIGINT,
    `mobile_app_id`: BIGINT
  >,
  `dismiss_auto_duty_still_in_pc_prompt_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `prompted_at_ms`: BIGINT,
    `still_in_pc`: BOOLEAN,
    `created_at_ms`: BIGINT,
    `vehicle_id`: BIGINT,
    `mobile_app_id`: BIGINT,
    `is_dismissed_by_driving`: BOOLEAN
  >,
  `dismiss_auto_duty_still_in_ym_prompt_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `prompted_at_ms`: BIGINT,
    `still_in_ym`: BOOLEAN,
    `created_at_ms`: BIGINT,
    `vehicle_id`: BIGINT,
    `mobile_app_id`: BIGINT
  >,
  `dismiss_auto_duty_ym_entry_prompt_request`: STRUCT<
    `org_id`: BIGINT,
    `driver_id`: BIGINT,
    `prompted_at_ms`: BIGINT,
    `created_at_ms`: BIGINT,
    `vehicle_id`: BIGINT,
    `mobile_app_id`: BIGINT,
    `new_duty_status`: INT
  >,
  `set_past_duty_status_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `log_at_ms`: BIGINT,
    `duty_status`: INT,
    `created_at_ms`: BIGINT,
    `old_duty_status`: INT,
    `vehicle_id`: BIGINT,
    `annotation`: STRING,
    `manual_location`: STRING,
    `mobile_app_id`: BIGINT
  >,
  `set_duty_status_for_time_range_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `start_ms`: BIGINT,
    `end_ms`: BIGINT,
    `duty_status`: INT,
    `created_at_ms`: BIGINT,
    `annotation`: STRING,
    `mobile_app_id`: BIGINT,
    `manual_input_location`: STRING
  >,
  `assign_driver_hos_unassigned_segments_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `vehicle_id`: BIGINT,
    `created_at_ms`: BIGINT,
    `new_duty_status`: INT,
    `segments`: ARRAY<
      STRUCT<
        `start_ms`: BIGINT,
        `vehicle_id`: BIGINT,
        `end_ms`: BIGINT,
        `annotation`: STRING,
        `duty_status`: INT,
        `manual_location`: STRING
      >
    >,
    `mobile_app_id`: BIGINT,
    `created_by_user_id`: BIGINT
  >,
  `delete_driver_hos_log_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `log_at_ms`: BIGINT,
    `deleted_at_ms`: BIGINT,
    `old_duty_status`: INT,
    `mobile_app_id`: BIGINT
  >,
  `clear_hos_annotated_segments_from_cloud_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `user_id`: BIGINT,
    `cleared_at_ms`: BIGINT,
    `segments`: ARRAY<
      STRUCT<
        `start_ms`: BIGINT,
        `vehicle_id`: BIGINT
      >
    >
  >,
  `annotate_hos_unassigned_segments_from_cloud_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `user_id`: BIGINT,
    `updated_at_ms`: BIGINT,
    `segments`: ARRAY<
      STRUCT<
        `start_ms`: BIGINT,
        `vehicle_id`: BIGINT,
        `end_ms`: BIGINT,
        `annotation`: STRING
      >
    >
  >,
  `edit_hos_annotated_segments_from_cloud_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `user_id`: BIGINT,
    `updated_at_ms`: BIGINT,
    `segments`: ARRAY<
      STRUCT<
        `start_ms`: BIGINT,
        `vehicle_id`: BIGINT,
        `annotation`: STRING
      >
    >
  >,
  `accept_reject_carrier_edits_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `day_start_ms`: BIGINT,
    `accept`: BOOLEAN
  >,
  `driver_bulk_accept_reject_carrier_edits_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `mobile_app_id`: BIGINT,
    `requested_at_ms`: BIGINT,
    `pending_carrier_edit`: ARRAY<
      STRUCT<
        `accept`: BOOLEAN,
        `pending_carrier_edit`: STRUCT<
          `start_ms`: BIGINT,
          `end_ms`: BIGINT,
          `current_status_code`: BIGINT,
          `pending_status_code`: BIGINT,
          `current_vehicle_id`: BIGINT,
          `pending_vehicle_id`: BIGINT,
          `created_at_ms`: BIGINT,
          `notes`: STRING,
          `created_by_user_id`: BIGINT,
          `team_driver_swap_uuid`: BINARY,
          `log_type`: BIGINT,
          `current_log_type`: BIGINT,
          `pending_log_type`: BIGINT,
          `current_status_code_override`: INT,
          `pending_status_code_override`: INT
        >,
        `malformed`: BOOLEAN
      >
    >
  >,
  `set_current_duty_status_by_public_api_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `driver_id`: BIGINT,
    `duty_status`: INT,
    `created_at_ms`: BIGINT,
    `vehicle_id`: BIGINT,
    `remarks`: STRING,
    `location`: STRING
  >,
  `patch_hos_unassigned_segment_by_public_api_request`: STRUCT<
    `org_id`: BIGINT,
    `group_id`: BIGINT,
    `unassigned_driving_segment_id`: STRING,
    `driver_id`: STRUCT<`driver_id`: BIGINT>,
    `annotation`: STRUCT<`annotation`: STRING>
  >,
  `submit_driver_hos_chart_metadata_request`: STRUCT<
    `org_id`: BIGINT,
    `driver_id`: BIGINT,
    `day_start_ms`: BIGINT,
    `eld_day_start_hour`: BIGINT,
    `timezone`: STRING,
    `submitted`: BOOLEAN,
    `shipping_docs`: STRING,
    `trailer_name`: STRING,
    `remarks`: ARRAY<
      STRUCT<
        `id`: BIGINT,
        `time_ms`: BIGINT,
        `notes`: STRING
      >
    >,
    `distance_override_miles`: BIGINT,
    `adverse_weather_enabled`: BOOLEAN,
    `big_day_enabled`: BOOLEAN,
    `defer_off_duty_enabled`: BOOLEAN,
    `canada_adverse_driving_enabled`: BOOLEAN,
    `utility_exemption_enabled`: BOOLEAN,
    `carrier_name`: STRING,
    `carrier_address`: STRING,
    `carrier_us_dot_number`: BIGINT,
    `home_terminal_name`: STRING,
    `home_terminal_address`: STRING,
    `has_submitted`: BOOLEAN,
    `has_shipping_docs`: BOOLEAN,
    `has_trailer_name`: BOOLEAN,
    `has_distance_override_miles`: BOOLEAN,
    `has_adverse_weather_enabled`: BOOLEAN,
    `has_big_day_enabled`: BOOLEAN,
    `has_defer_off_duty_enabled`: BOOLEAN,
    `has_canada_adverse_driving_enabled`: BOOLEAN,
    `has_utility_exemption_enabled`: BOOLEAN,
    `has_carrier_name`: BOOLEAN,
    `has_carrier_address`: BOOLEAN,
    `has_carrier_us_dot_number`: BOOLEAN,
    `has_home_terminal_name`: BOOLEAN,
    `has_home_terminal_address`: BOOLEAN,
    `deferral_off_duty_metadata`: STRUCT<
      `day_one_confirmation_ms`: BIGINT,
      `deferral_duration_ms`: BIGINT,
      `day_two_confirmation_ms`: BIGINT,
      `day_two_prompt_shown`: BOOLEAN
    >,
    `additional_hours_option`: INT,
    `additional_hours_option_updated_by_user_id`: BIGINT,
    `additional_hours_option_updated_by_driver_id`: BIGINT,
    `vehicle_name_overrides`: ARRAY<
      STRUCT<
        `vehicle_id`: BIGINT,
        `vehicle_name_override`: STRING
      >
    >,
    `submitted_ms`: BIGINT
  >,
  `certify_driver_hos_charts_request`: STRUCT<
    `org_id`: BIGINT,
    `driver_id`: BIGINT,
    `charts`: ARRAY<
      STRUCT<
        `day_start_ms`: BIGINT,
        `timezone`: STRING,
        `eld_day_start_hour`: BIGINT,
        `submitted`: BOOLEAN,
        `carrier_name`: STRING,
        `carrier_address`: STRING,
        `carrier_us_dot_number`: BIGINT,
        `home_terminal_name`: STRING,
        `home_terminal_address`: STRING,
        `remarks`: ARRAY<
          STRUCT<
            `id`: BIGINT,
            `time_ms`: BIGINT,
            `notes`: STRING
          >
        >
      >
    >,
    `submitted_ms`: BIGINT
  >,
  `leave_vehicle_request`: STRUCT<
    `org_id`: BIGINT,
    `driver_id`: BIGINT,
    `vehicle_id`: BIGINT,
    `time_ms`: BIGINT,
    `session_uuid`: STRING,
    `mobile_app_id`: BIGINT
  >,
  `claim_exemption_as_driver_for_time_request`: STRUCT<
    `org_id`: BIGINT,
    `driver_id`: BIGINT,
    `applies_at_ms`: BIGINT,
    `exemption_type_id`: BIGINT,
    `remark`: STRING,
    `client_created_at_ms`: BIGINT,
    `mobile_app_id`: BIGINT
  >,
  `unclaim_exemption_as_driver_for_time_request`: STRUCT<
    `org_id`: BIGINT,
    `driver_id`: BIGINT,
    `applies_at_ms`: BIGINT,
    `exemption_type_id`: BIGINT,
    `client_deleted_at_ms`: BIGINT,
    `mobile_app_id`: BIGINT,
    `client_updated_at_ms`: BIGINT
  >,
  `activate_agriculture_exemption_request`: STRUCT<
    `org_id`: BIGINT,
    `driver_id`: BIGINT,
    `created_at_ms`: BIGINT,
    `vehicle_id`: BIGINT,
    `mobile_app_id`: BIGINT
  >,
  `deactivate_agriculture_exemption_request`: STRUCT<
    `org_id`: BIGINT,
    `driver_id`: BIGINT,
    `created_at_ms`: BIGINT,
    `vehicle_id`: BIGINT,
    `mobile_app_id`: BIGINT
  >,
  `log_edit_diff`: STRUCT<
    `before`: ARRAY<
      STRUCT<
        `duty_status`: INT,
        `time_ms`: BIGINT,
        `vehicle_id`: BIGINT,
        `remark`: STRING,
        `location`: STRING
      >
    >,
    `after`: ARRAY<
      STRUCT<
        `duty_status`: INT,
        `time_ms`: BIGINT,
        `vehicle_id`: BIGINT,
        `remark`: STRING,
        `location`: STRING
      >
    >,
    `mobile_app_id`: BIGINT,
    `initiated_by_driver_id`: BIGINT
  >
>,
`_raw_proto` STRING,
`action_uuid` STRING,
`mobile_app_id` BIGINT,
`date` STRING
