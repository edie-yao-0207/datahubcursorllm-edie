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
    `gateway_micro_config`: STRUCT<
      `org_id`: BIGINT,
      `config_version`: BIGINT,
      `gateway_firmware`: STRING,
      `sleep_duration_sec`: BIGINT,
      `sleep_disabled`: BOOLEAN,
      `gps_duration_sec`: BIGINT,
      `gateway_firmware_sha256`: BINARY,
      `modem_firmware`: STRING,
      `http_timeout_sec`: BIGINT,
      `wake_on_motion_enabled`: BOOLEAN,
      `wake_on_motion_config`: STRUCT<
        `in_movement_sleep_duration_sec`: BIGINT,
        `start_stop_min_interval_sec`: BIGINT,
        `use_in_movement_sleep_duration_sec`: BOOLEAN,
        `lis2dh_interrupt_config`: STRUCT<
          `interrupt_threshold_mg`: BIGINT,
          `interrupt_duration_ms`: BIGINT
        >,
        `start_ping_threshold_sec`: BIGINT,
        `start_ping_window_sec`: BIGINT,
        `stop_ping_threshold_sec`: BIGINT,
        `stop_ping_window_sec`: BIGINT
      >,
      `send_local_diagnostic_logs`: BOOLEAN,
      `scheduled_wake`: STRUCT<
        `loop_start_utc_ms`: BIGINT,
        `loop_duration_ms`: BIGINT,
        `sleep_duration_sec`: ARRAY<BIGINT>
      >,
      `force_ota_upgrade`: BOOLEAN,
      `ota_http_timeout_sec`: BIGINT,
      `scan_network`: BOOLEAN,
      `deactivate`: BIGINT,
      `enable_gps_instrumentation`: BOOLEAN,
      `enable_airplane_mode_during_gps_scan`: BOOLEAN,
      `gps_max_fix_count`: BIGINT,
      `gps_stationary_snap_m`: BIGINT,
      `enable_wifi_ap_scan`: BOOLEAN,
      `wifi_ap_scan_ignores_gps`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
