`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`id` BIGINT,
`name` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`auto_add_user_domain` STRING,
`temp_units` INT,
`join_fleet_token` STRING,
`logo_type` INT,
`logo_domain` STRING,
`logo_s3_location` STRING,
`login_domain` STRING,
`voice_coaching_enabled` BYTE,
`voice_coaching_speed_threshold_milliknots` BIGINT,
`gamification_enabled` BYTE,
`quarantine_enabled` BYTE,
`locale` STRING,
`unit_system_override` STRING,
`language_override` STRING,
`currency_override` STRING,
`quarantine_enabled_at` DATE,
`vibration_units` INT,
`message_push_notifications_enabled` BYTE,
`audio_recording_enabled` BYTE,
`trailer_selection_enabled` BYTE,
`hotspot_unblock_deadline` TIMESTAMP,
`wifi_hotspot_data_cap_bytes_per_gateway` BIGINT,
`message_text_to_speech_enabled` BYTE,
`wake_on_motion_enabled` BYTE,
`rollout_stage` SHORT,
`driver_dispatch_manual_events_enabled` BYTE,
`driver_route_self_assignment_enabled` BYTE,
`settings_proto` STRUCT<
  `machine_vision_image_upload_rate_limiter_config`: STRUCT<
    `low_res_success_pixels_per_second`: BIGINT,
    `low_res_failure_pixels_per_second`: BIGINT,
    `high_res_success_pixels_per_second`: BIGINT,
    `high_res_failure_pixels_per_second`: BIGINT
  >,
  `face_detection_enabled`: BOOLEAN,
  `nvr_shutoff_config`: STRUCT<`nvr_shutoff_delay_minutes`: BIGINT>,
  `driver_facing_video_enabled`: BOOLEAN,
  `dvir_pre_or_post_trip_required`: BOOLEAN,
  `dvir_defect_comments_required`: BOOLEAN,
  `sharp_turn_severity_enabled`: BOOLEAN,
  `sharp_turn_severity_g_threshold`: FLOAT,
  `filter_out_all_sharp_turns`: BOOLEAN,
  `safety_email_notification_enabled`: BOOLEAN,
  `harsh_event_severity_settings`: STRUCT<
    `harsh_event_severity_settings_enabled`: BOOLEAN,
    `filter_out_all_harsh_turns`: BOOLEAN,
    `filter_out_all_harsh_accels`: BOOLEAN,
    `filter_out_all_harsh_brakes`: BOOLEAN
  >,
  `forward_collision_warning_enabled`: BOOLEAN,
  `driver_facing_camera_disabled`: BOOLEAN,
  `distracted_driving_detection_enabled`: BOOLEAN,
  `forward_collision_warning_audio_alerts_enabled`: BOOLEAN,
  `distracted_driving_audio_alerts_enabled`: BOOLEAN,
  `disallow_driver_dvir_resolution`: BOOLEAN,
  `distracted_driving_sensitivity_level`: INT,
  `forward_collision_sensitivity_level`: INT,
  `edit_certified_hos_logs_enabled`: BOOLEAN,
  `harsh_event_video_length_seconds`: INT,
  `unregulated_vehicles_enabled`: BOOLEAN,
  `whitelisted_ips`: ARRAY<STRING>,
  `device_audio_language`: INT,
  `voice_coaching_alert_config`: STRUCT<
    `seat_belt_unbuckled_alert_disabled`: BOOLEAN,
    `maximum_speed_alert_disabled`: BOOLEAN,
    `harsh_driving_alert_disabled`: BOOLEAN,
    `crash_alert_disabled`: BOOLEAN,
    `maximum_speed_time_before_alert_ms`: BIGINT
  >,
  `rfid_auto_end_driver_assignments_ms`: BIGINT,
  `vg_custom_low_power_profile`: STRUCT<
    `low_power_enabled`: BOOLEAN,
    `override_timeout_secs`: BIGINT,
    `voltage_threshold`: BIGINT,
    `enable_for_tag_ids`: ARRAY<BIGINT>,
    `modified_during_battery_conservation_mode_migration_03_29_2021`: BOOLEAN
  >,
  `following_distance_enabled`: BOOLEAN,
  `dvir_has_sanitation_defect_flow`: BOOLEAN,
  `dvir_has_sanitation_defect_flow_auto_inject`: BOOLEAN,
  `camera_height_meters`: FLOAT,
  `following_distance_audio_alerts_enabled`: BOOLEAN,
  `adas_all_features_allowed`: BOOLEAN,
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
    `coaching_settings`: STRUCT<
      `batched_coaching_enabled`: BOOLEAN,
      `safety_event_auto_triage_enabled`: BOOLEAN,
      `safety_labels_to_automatically_triage`: ARRAY<INT>
    >,
    `recording_settings`: STRUCT<
      `cm_31_storage_hours_setting`: BIGINT,
      `cm_32_storage_hours_setting`: BIGINT,
      `high_res_storage_percent_setting`: BIGINT,
      `parking_mode`: STRUCT<`duration_seconds`: BIGINT>,
      `ahd_1_storage_hours_setting`: BIGINT,
      `cm_33_storage_hours_setting`: BIGINT,
      `cm_34_storage_hours_setting`: BIGINT,
      `has_2k_resolution_enabled`: BOOLEAN
    >,
    `distracted_driving_detection_settings`: STRUCT<
      `minimum_speed_enum`: INT,
      `minimum_duration_enum`: INT
    >,
    `following_distance_settings`: STRUCT<
      `minimum_speed_enum`: INT,
      `minimum_duration_enum`: INT,
      `minimum_following_distance_seconds`: FLOAT
    >,
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
    `camera_id_settings`: STRUCT<
      `confirmation_mode_enabled`: BOOLEAN,
      `autoassignment_groups_enabled`: BOOLEAN,
      `sensitive_region_processing_enabled`: BOOLEAN,
      `automatic_training_disabled`: BOOLEAN,
      `autoassignment_tag_based_enabled`: BOOLEAN
    >,
    `livestream_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `outward_camera_enabled`: BOOLEAN,
      `inward_camera_enabled`: BOOLEAN,
      `allowance_secs`: BIGINT,
      `outward_camera_settings`: STRUCT<
        `bits_per_second`: BIGINT,
        `frames_per_second`: BIGINT,
        `resolution`: INT
      >,
      `inward_camera_settings`: STRUCT<
        `bits_per_second`: BIGINT,
        `frames_per_second`: BIGINT,
        `resolution`: INT
      >,
      `max_session_duration_seconds`: BIGINT,
      `audio_stream_enabled`: BOOLEAN,
      `helicopter_view_enabled`: BOOLEAN
    >,
    `gamification_settings`: STRUCT<`anonymize_driver_names_for_drivers`: BOOLEAN>,
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
    `event_review_settings`: STRUCT<
      `skip_manual_review`: BOOLEAN,
      `safety_labels_to_skip_manual_review`: ARRAY<INT>,
      `show_pending_folder`: BOOLEAN
    >,
    `asset_privacy_settings`: STRUCT<`asset_privacy_checks_enabled`: BOOLEAN>,
    `speeding_settings`: STRUCT<
      `max_speed_setting`: STRUCT<
        `enabled`: BOOLEAN,
        `time_before_alert_ms`: BIGINT,
        `kmph_threshold`: FLOAT,
        `in_cab_audio_alerts_enabled`: BOOLEAN,
        `send_to_safety_inbox`: BOOLEAN,
        `auto_add_to_coaching`: BOOLEAN
      >,
      `in_cab_severity_alert_setting`: STRUCT<
        `enabled`: BOOLEAN,
        `alert_at_severity_level`: INT,
        `time_before_alert_ms`: BIGINT
      >,
      `severity_settings_speed_over_limit_unit`: INT,
      `severity_settings`: ARRAY<
        STRUCT<
          `severity_level`: INT,
          `speed_over_limit_threshold`: FLOAT,
          `time_before_alert_ms`: BIGINT,
          `enabled`: BOOLEAN,
          `send_to_safety_inbox`: BOOLEAN,
          `auto_add_to_coaching`: BOOLEAN,
          `evidence_based_speeding_enabled`: BOOLEAN
        >
      >,
      `csl_enabled`: BOOLEAN
    >,
    `drowsiness_detection_settings`: STRUCT<
      `drowsiness_detection_enabled`: BOOLEAN,
      `drowsiness_detection_audio_alerts_enabled`: BOOLEAN,
      `drowsiness_detection_sensitivity_level`: INT
    >,
    `lane_departure_warning_settings`: STRUCT<
      `lane_departure_warning_enabled`: BOOLEAN,
      `lane_departure_warning_audio_alerts_enabled`: BOOLEAN
    >
  >,
  `sled_settings`: STRUCT<`spreaders_parent_tag_id`: BIGINT>,
  `dvir_settings`: STRUCT<
    `use_mandatory_checklist_flow`: BOOLEAN,
    `disallow_driver_defect_resolution`: BOOLEAN,
    `enable_defect_resolution_not_needed`: BOOLEAN,
    `disallow_driver_choose_image_from_library`: BOOLEAN,
    `all_walkaround_photos_required`: BOOLEAN,
    `view_all_inspected_defects`: BOOLEAN,
    `pre_trip_dvir_required`: BOOLEAN,
    `post_trip_dvir_required`: BOOLEAN,
    `require_at_least_one_defect_to_mark_unsafe`: BOOLEAN,
    `major_defect_settings`: STRUCT<
      `enabled`: BOOLEAN,
      `dvir_unsafe_with_major_defect`: BOOLEAN,
      `require_at_least_one_major_defect_to_mark_unsafe`: BOOLEAN
    >,
    `enable_height_verification`: BOOLEAN
  >,
  `organization_compliance_settings`: STRUCT<
    `force_manual_location_for_duty_status_changes`: BOOLEAN,
    `auto_duty_ym_speed_limit_enabled`: BOOLEAN,
    `auto_duty_pc_daily_distance_limit_enabled`: BOOLEAN,
    `canada_eld_mandate_enabled`: BOOLEAN,
    `off_duty_dvir_enabled`: BOOLEAN,
    `auto_duty_ym_persist_after_engine_restart`: BOOLEAN,
    `auto_duty_ym_entry_prompt`: BOOLEAN
  >,
  `organization_driver_app_settings`: STRUCT<
    `use_premade_signin_wizard`: BOOLEAN,
    `allow_vehicle_search_outside_of_tag`: BOOLEAN,
    `allow_trailer_search_outside_of_tag`: BOOLEAN,
    `customizable_driver_features_settings`: STRUCT<
      `hos_enabled`: BOOLEAN,
      `dvir_enabled`: BOOLEAN,
      `route_enabled`: BOOLEAN,
      `document_enabled`: BOOLEAN,
      `messaging_enabled`: BOOLEAN,
      `team_driving_enabled`: BOOLEAN,
      `custom_tile_config`: ARRAY<
        STRUCT<
          `id`: BIGINT,
          `tile_name`: STRING,
          `url`: STRING,
          `color`: STRING
        >
      >,
      `driver_scores_enabled`: BOOLEAN,
      `eco_driving_score_enabled`: BOOLEAN,
      `safety_score_ranking_enabled`: BOOLEAN,
      `eu_hos_enabled`: BOOLEAN,
      `trip_log_enabled`: BOOLEAN,
      `allow_manual_trip_purpose_classification`: BOOLEAN,
      `auto_classify_trips_as_private`: BOOLEAN,
      `self_coaching_disabled`: BOOLEAN,
      `allow_asset_search`: BOOLEAN,
      `training_tile_disabled`: BOOLEAN,
      `telematics_forms_disabled`: BOOLEAN,
      `navigation_enabled`: BOOLEAN,
      `qualifications_tile_disabled`: BOOLEAN,
      `navigation_speed_limit_disabled`: BOOLEAN,
      `worker_safety_sos_enabled`: BOOLEAN,
      `navigation_disabled`: BOOLEAN,
      `drivewyze_settings`: STRUCT<
        `enabled`: BOOLEAN,
        `login_entity_id`: STRING
      >,
      `work_orders_enabled`: BOOLEAN,
      `technician_time_tracking_enabled`: BOOLEAN,
      `remote_privacy_button_enabled`: BOOLEAN
    >,
    `route_stop_arrival_wizard_enabled`: BOOLEAN,
    `use_location_based_vehicle_selection`: BOOLEAN,
    `nearby_vehicles_distance_limit`: BIGINT
  >,
  `branded_reports_enabled`: BOOLEAN,
  `branded_reports_settings`: STRUCT<
    `sender_name`: STRING,
    `reply_to_address`: STRING,
    `business_address`: STRING,
    `phone_number`: STRING,
    `website`: STRING
  >,
  `organization_gateway_settings`: STRUCT<`ecu_pto_enabled`: BOOLEAN>,
  `industrial`: STRUCT<
    `start_time_of_day_ms`: BIGINT,
    `roc_time_sync_enabled`: BOOLEAN
  >,
  `report_settings`: STRUCT<`use_old_activity_report`: BOOLEAN>,
  `account_settings`: STRUCT<
    `hide_referral_link`: BOOLEAN,
    `full_dashboard_trial`: BOOLEAN
  >,
  `acc_rs232_settings`: STRUCT<`application`: INT>,
  `organization_sim_settings`: STRUCT<
    `sim_product_group_settings`: ARRAY<
      STRUCT<
        `product_group`: BIGINT,
        `sim_slot_failover_disabled`: INT
      >
    >
  >,
  `fuel_settings`: STRUCT<`fuel_purchase_upload_date_format`: INT>,
  `remote_privacy_button_settings`: STRUCT<
    `enabled`: BOOLEAN,
    `default_privacy_mode_enabled`: BOOLEAN
  >
>,
`_raw_settings_proto` STRING,
`driver_facing_trip_images_enabled` BYTE,
`rolling_stop_threshold_milliknots` BIGINT,
`rolling_stop_enabled` BYTE,
`driver_trailer_creation_enabled` BYTE,
`max_num_of_trailers_selected` BYTE,
`internal_type` BIGINT,
`is_early_adopter` BYTE,
`release_type_enum` BYTE,
`partition` STRING
