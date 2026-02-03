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
    `cable_upgrade`: STRUCT<
      `cable_type`: INT,
      `upgrade_status`: ARRAY<INT>,
      `existing_cable_firmware_crc`: STRING,
      `new_cable_firmware_crc`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
