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
    `cm3x_pmic_reset_reasons`: STRUCT<
      `on_reason`: INT,
      `on_reason1`: INT,
      `warm_reset_reason`: INT,
      `off_reason`: INT,
      `off_reason1`: INT,
      `fault_reason1`: INT,
      `fault_reason2`: INT,
      `s3_reset_reason`: INT,
      `soft_reset_reason`: INT,
      `pmic`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
