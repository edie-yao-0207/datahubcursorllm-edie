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
    `mcu_app_event`: STRUCT<
      `raw_event`: STRUCT<
        `gateway_micro_app_event`: STRUCT<
          `mcu_app_event`: INT,
          `metadata`: BIGINT,
          `program_counter`: BIGINT,
          `link_register`: BIGINT,
          `utc_ms`: BIGINT,
          `mcu_had_utc`: BOOLEAN,
          `rtc_ms`: BIGINT,
          `boot_count`: BIGINT,
          `sequence_number`: BIGINT,
          `gateway_firmware_hash`: BINARY,
          `modem_firmware_version`: BINARY
        >
      >,
      `mcu_app_event`: INT,
      `metadata`: BIGINT,
      `program_counter`: BIGINT,
      `link_register`: BIGINT,
      `utc_ms`: BIGINT,
      `utc_is_estimated`: BOOLEAN,
      `mcu_firmware`: STRING,
      `modem_firmware`: STRING,
      `boot_count`: BIGINT,
      `gateway_time_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
