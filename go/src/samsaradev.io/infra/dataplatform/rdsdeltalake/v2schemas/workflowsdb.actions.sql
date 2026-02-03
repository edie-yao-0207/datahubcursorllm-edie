`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`uuid` STRING,
`org_id` BIGINT,
`workflow_uuid` STRING,
`type` INT,
`proto_data` STRUCT<
  `time_range_settings`: STRUCT<
    `applies_in_range`: BOOLEAN,
    `time_ranges`: ARRAY<
      STRUCT<
        `days_of_week`: ARRAY<INT>,
        `start_time_of_day_ms`: DECIMAL(20, 0),
        `end_time_of_day_ms`: DECIMAL(20, 0)
      >
    >
  >,
  `notification`: STRUCT<
    `recipients`: ARRAY<
      STRUCT<
        `recipient_type`: INT,
        `recipient_id`: BIGINT,
        `enabled_notification_types`: ARRAY<INT>,
        `bypass_dnd`: BOOLEAN
      >
    >
  >,
  `safety_coaching`: STRUCT<`coachable_item_type`: INT>,
  `dashboard_notification`: STRUCT<`should_notify_fleet_overview`: BOOLEAN>,
  `webhook`: STRUCT<
    `webhook_configs`: ARRAY<
      STRUCT<`webhook_id`: BIGINT>
    >,
    `payload_type`: INT
  >,
  `driver_app_notification`: STRUCT<
    `should_send_blocking_alert`: BOOLEAN,
    `should_dictate_alert_title`: BOOLEAN,
    `should_play_alert_sound`: BOOLEAN,
    `custom_text`: STRING,
    `should_send_push_notification`: BOOLEAN,
    `should_send_in_app_notification`: BOOLEAN
  >,
  `assign_form`: STRUCT<
    `form_template_uuid`: BINARY,
    `due_after`: STRUCT<
      `number_of_intervals`: BIGINT,
      `time_interval`: INT
    >,
    `assigned_by_id`: BIGINT,
    `assigned_to_polymorphic_ids`: ARRAY<BIGINT>,
    `assigned_to_tag_ids`: ARRAY<BIGINT>,
    `automatically_assign_to_driver_associated_with_incident`: BOOLEAN
  >,
  `engine_immobilization`: STRUCT<
    `relays`: ARRAY<
      STRUCT<
        `id`: INT,
        `relay_state`: INT
      >
    >
  >,
  `assign_attribute_values`: STRUCT<
    `attribute_values`: ARRAY<
      STRUCT<
        `attribute_id`: BINARY,
        `attribute_value_id`: BINARY,
        `double_value`: DOUBLE,
        `string_value`: STRING
      >
    >
  >,
  `rapid_sos`: STRUCT<
    `sos_recipients`: ARRAY<
      STRUCT<
        `recipient_type`: INT,
        `recipient_id`: BIGINT
      >
    >,
    `direct_to_911_enabled`: BOOLEAN,
    `verified_escalation_enabled`: BOOLEAN
  >,
  `assign_training`: STRUCT<
    `course_uuids`: ARRAY<BINARY>,
    `due_after`: STRUCT<
      `number_of_intervals`: BIGINT,
      `time_interval_units`: INT
    >,
    `condition`: STRUCT<
      `number_of_intervals`: BIGINT,
      `time_interval_units`: INT
    >,
    `assigned_by`: BIGINT,
    `assigned_to_polymorphic_ids`: ARRAY<BIGINT>,
    `assigned_to_tag_ids`: ARRAY<BIGINT>,
    `automatically_assign_to_driver_associated_with_incident`: BOOLEAN
  >,
  `send_to_coaching`: STRUCT<
    `coaching_type`: INT,
    `share_with_driver`: BOOLEAN,
    `email_coach`: INT
  >,
  `script_execution`: STRUCT<
    `scriptexecution_configs`: ARRAY<
      STRUCT<`script_name`: STRING>
    >
  >,
  `automation_trigger`: STRUCT<`automation_config_id`: STRING>,
  `request_check_in`: STRUCT<
    `duration_ms`: BIGINT,
    `use_ai_agent_verification`: BOOLEAN,
    `assigned_by`: BIGINT,
    `assigned_to_polymorphic_ids`: ARRAY<BIGINT>,
    `assigned_to_tag_ids`: ARRAY<BIGINT>,
    `automatically_assign_to_driver_associated_with_incident`: BOOLEAN
  >,
  `send_gsoc_notification`: STRUCT<
    `escalate_if_no_acknowledgement_after_ms`: BIGINT,
    `instructions`: STRING
  >
>,
`_raw_proto_data` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`deleted_at` TIMESTAMP,
`deleted_by_user_id` BIGINT,
`repeat_ms` BIGINT,
`partition` STRING
