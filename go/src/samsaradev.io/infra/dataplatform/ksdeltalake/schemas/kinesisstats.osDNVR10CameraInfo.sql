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
    `nvr10_camera_info`: STRUCT<
      `device`: ARRAY<
        STRUCT<
          `product_id`: STRING,
          `serial_number`: STRING,
          `board_revision`: BIGINT,
          `mfg_serial_number`: STRING,
          `mfg_time`: DECIMAL(20, 0),
          `mfg_location`: BIGINT,
          `stream_id`: DECIMAL(20, 0),
          `camera_device_id`: DECIMAL(20, 0),
          `firmware_version`: STRING
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
