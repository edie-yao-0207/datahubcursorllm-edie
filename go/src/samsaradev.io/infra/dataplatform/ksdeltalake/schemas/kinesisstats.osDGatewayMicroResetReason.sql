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
    `gateway_micro_reset_reason`: STRUCT<
      `pin_reset`: BOOLEAN,
      `watchdog_reset`: BOOLEAN,
      `soft_reset`: BOOLEAN,
      `lockup_reset`: BOOLEAN,
      `gpio_reset`: BOOLEAN,
      `lpcomp_reset`: BOOLEAN,
      `debug_reset`: BOOLEAN,
      `nfc_reset`: BOOLEAN,
      `vbus_reset`: BOOLEAN,
      `modem_reset`: BOOLEAN,
      `power_reset`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
