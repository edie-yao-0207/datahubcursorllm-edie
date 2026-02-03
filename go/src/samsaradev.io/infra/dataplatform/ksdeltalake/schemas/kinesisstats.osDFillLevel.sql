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
    `fill_level`: STRUCT<
      `fill_millipercent`: INT,
      `fill_volume_milliliters`: BIGINT,
      `fill_mass_grams`: BIGINT,
      `available_capacity_volume_milliliters`: BIGINT,
      `available_capacity_mass_grams`: BIGINT,
      `criticality`: INT,
      `smoothed_fill_volume_milliliters`: BIGINT,
      `smoothed_fill_mass_grams`: BIGINT,
      `smoothed_fill_millipercent`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
