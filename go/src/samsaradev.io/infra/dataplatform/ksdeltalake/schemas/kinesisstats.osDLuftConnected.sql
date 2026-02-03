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
    `luft_connected`: STRUCT<
      `serial_number`: STRING,
      `firmware_version_reefer_port`: STRING,
      `firmware_version_display_port`: STRING,
      `firmware_version_tachograph_port`: STRING,
      `mcu_revision`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
