`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`id` BIGINT,
`org_id` BIGINT,
`driver_id` BIGINT,
`start_at` TIMESTAMP,
`end_at` TIMESTAMP,
`submitted` BYTE,
`timezone` STRING,
`driver_logs` STRING,
`vehicle_assignments` STRUCT<
  `assignments`: ARRAY<
    STRUCT<
      `start_ms`: BIGINT,
      `end_ms`: BIGINT,
      `vehicle_id`: BIGINT,
      `log_type`: BIGINT,
      `id`: BIGINT,
      `org_id`: BIGINT,
      `driver_id`: BIGINT,
      `is_passenger`: BOOLEAN,
      `codriver_records`: ARRAY<
        STRUCT<
          `codriver_id`: BIGINT,
          `start_ms`: BIGINT,
          `end_ms`: BIGINT
        >
      >,
      `force_close_existing_req`: STRUCT<
        `assignment_id`: BIGINT,
        `driver_id`: BIGINT,
        `logged_in_at_ms`: BIGINT
      >,
      `session_uuid`: STRING,
      `created_at`: BIGINT
    >
  >
>,
`_raw_vehicle_assignments` STRING,
`submitted_at` TIMESTAMP,
`shipping_docs` STRING,
`trailer_name` STRING,
`server_driver_logs` STRING,
`remarks` STRUCT<
  `remarks`: ARRAY<
    STRUCT<
      `id`: BIGINT,
      `time_ms`: BIGINT,
      `notes`: STRING
    >
  >
>,
`_raw_remarks` STRING,
`distance_override_meters` BIGINT,
`carrier_name` STRING,
`carrier_address` STRING,
`carrier_us_dot_number` BIGINT,
`chart_proto` STRUCT<
  `home_terminal_name`: STRING,
  `home_terminal_address`: STRING,
  `deferral_off_duty_metadata`: STRUCT<
    `day_one_confirmation_ms`: BIGINT,
    `deferral_duration_ms`: BIGINT,
    `day_two_confirmation_ms`: BIGINT,
    `day_two_prompt_shown`: BOOLEAN
  >,
  `additional_hours_option`: INT,
  `additional_hours_option_updated_at_ms`: BIGINT,
  `additional_hours_option_updated_by_user_id`: BIGINT,
  `additional_hours_option_updated_by_driver_id`: BIGINT,
  `vehicle_name_overrides`: ARRAY<
    STRUCT<
      `vehicle_id`: BIGINT,
      `vehicle_name_override`: STRING
    >
  >
>,
`_raw_chart_proto` STRING,
`big_day_enabled` BYTE,
`adverse_weather_enabled` BYTE,
`certification_events` STRUCT<
  `certification_events`: ARRAY<
    STRUCT<`submitted_at`: BIGINT>
  >
>,
`_raw_certification_events` STRING,
`defer_off_duty_enabled` BYTE,
`canada_adverse_driving_enabled` BYTE,
`utility_exemption_enabled` BYTE,
`recertification_requested_by_autoduty_at` TIMESTAMP,
`date` STRING
