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
    `modi_device_info`: STRUCT<
      `manufacturing_data`: STRUCT<
        `product_id`: BIGINT,
        `board_revision`: BIGINT,
        `mfg_date_ms`: BIGINT,
        `mfg_location`: BIGINT,
        `build_phase`: BIGINT,
        `product_version`: BIGINT,
        `bom_version`: BIGINT
      >,
      `firmware_versions`: ARRAY<
        STRUCT<
          `type`: INT,
          `major_version`: BIGINT,
          `minor_version`: BIGINT,
          `patch`: BIGINT,
          `commit_hash`: STRING
        >
      >,
      `hardware_data`: STRUCT<
        `has_hardware_id`: BOOLEAN,
        `hardware_id`: BIGINT,
        `mcu_model`: INT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
