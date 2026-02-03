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
    `hardware_info`: STRUCT<
      `hardware_version`: STRING,
      `product_version`: STRING,
      `build_phase`: STRING,
      `bill_of_materials_version`: STRING,
      `product`: STRING,
      `variant`: STRING,
      `mac`: STRING,
      `serial`: STRING,
      `octo4_hw_info`: STRUCT<
        `ddr_vendor`: STRING,
        `emmc_vendor`: STRING
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
