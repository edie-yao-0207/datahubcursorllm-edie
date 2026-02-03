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
    `gateway_micro_diagnostic_status`: STRUCT<
      `local_sec`: BIGINT,
      `gps_sec`: BIGINT,
      `modem_sec`: BIGINT,
      `gateway_firmware`: STRING,
      `planned_sleep_duration_sec`: BIGINT,
      `org_id`: BIGINT,
      `battery_mv`: BIGINT,
      `modem_firmware`: STRING,
      `time_awake_ms`: BIGINT,
      `boot_count`: BIGINT,
      `watchdog_reset_count`: BIGINT,
      `bootloader_firmware`: STRING,
      `gps_scan_total_sec`: BIGINT,
      `modem_connect_additional_sec`: BIGINT,
      `modem_reset_count`: BIGINT,
      `activated`: BOOLEAN,
      `enroll_duration_sec`: BIGINT,
      `prev_post_duration_sec`: BIGINT,
      `modem_power_fail_count`: BIGINT,
      `battery_stats_awake`: ARRAY<
        STRUCT<
          `battery_stats_state`: INT,
          `min_mv`: BIGINT,
          `max_mv`: BIGINT,
          `avg_mv`: BIGINT,
          `cell_id`: BIGINT
        >
      >,
      `wake_up_count`: BIGINT,
      `battery_mv_1`: BIGINT,
      `battery_mv_2`: BIGINT,
      `battery_mv_3`: BIGINT,
      `rtc_status`: STRUCT<
        `nordic_rtc_utc_diff_ms`: INT,
        `gps_first_fix_utc_ms`: BIGINT,
        `modem_utc_ms`: BIGINT,
        `clock_source_used`: INT,
        `temperature_correction_offset_ms`: BIGINT
      >,
      `in_hibernation`: BOOLEAN,
      `gps_scan_skipped_no_accel_movement`: BOOLEAN,
      `wifi_scan_time_ms`: BIGINT,
      `total_time_awake_prev_ping_ms`: BIGINT,
      `registration_time_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
