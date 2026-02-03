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
    `modi_usb_thermal_shutoff_info`: STRUCT<
      `thermal_shutoff_enabled`: BOOLEAN,
      `vg34_thermistor_temp_millic`: BIGINT,
      `camera_usb_port_enabled_during_shutoff`: BOOLEAN
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
