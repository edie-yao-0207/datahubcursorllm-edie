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
    `vg_mcu_metrics`: STRUCT<
      `metrics_mcu_time_ms`: BIGINT,
      `vg_processed_time_utc_ms`: BIGINT,
      `vg_fw_build`: STRING,
      `mcu_fw_version`: STRING,
      `boot_count`: BIGINT,
      `interval_duration_ms`: BIGINT,
      `reset_reason`: STRUCT<
        `stop_acknowledge_error`: BOOLEAN,
        `mdm_ap_system_reset_request`: BOOLEAN,
        `software`: BOOLEAN,
        `core_lockup`: BOOLEAN,
        `jtag`: BOOLEAN,
        `power_on`: BOOLEAN,
        `external_pin`: BOOLEAN,
        `watchdog`: BOOLEAN,
        `pll_loss_of_lock`: BOOLEAN,
        `scg_loss_of_clock`: BOOLEAN,
        `low_high_voltage_detect`: BOOLEAN,
        `illegal_low_power`: BOOLEAN,
        `window_watchdog`: BOOLEAN,
        `independent_watchdog`: BOOLEAN,
        `brown_out`: BOOLEAN,
        `d2_domain_switch`: BOOLEAN,
        `d1_domain_switch`: BOOLEAN,
        `cpu_reset`: BOOLEAN,
        `mpu_error`: BOOLEAN,
        `sram_ecc_or_parity_error`: BOOLEAN,
        `trustzone_error`: BOOLEAN,
        `cache_parity_error`: BOOLEAN
      >,
      `task_usage`: ARRAY<
        STRUCT<
          `task_index`: INT,
          `task_name`: STRING,
          `interval_cpu_usage_ms`: BIGINT,
          `interval_cpu_usage_percent`: FLOAT,
          `boot_cpu_usage_ms`: BIGINT,
          `boot_cpu_usage_percent`: FLOAT,
          `boot_stack_usage_bytes`: BIGINT,
          `boot_stack_usage_percent`: BIGINT,
          `max_stack_size_bytes`: BIGINT
        >
      >,
      `imprecise_timestamp`: BOOLEAN,
      `mcu_serial_number`: BIGINT,
      `serial_number_string`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
