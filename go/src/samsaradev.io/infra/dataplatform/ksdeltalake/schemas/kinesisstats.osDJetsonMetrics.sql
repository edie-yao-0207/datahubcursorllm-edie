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
    `jetson_metrics`: STRUCT<
      `reset_reason`: STRUCT<
        `pmc_reset_level`: INT,
        `pmic_register`: INT,
        `pmc_reset_source`: INT,
        `pmic_reset_reasons`: ARRAY<INT>,
        `tegra_system_reset_reason`: INT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
