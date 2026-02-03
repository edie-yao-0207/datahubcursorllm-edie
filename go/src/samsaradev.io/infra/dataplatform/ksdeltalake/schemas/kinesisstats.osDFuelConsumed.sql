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
    `fuel_consumption_info`: STRUCT<
      `tracking_method`: INT,
      `fuel_type`: INT,
      `fuel_tracker_infos`: ARRAY<
        STRUCT<
          `tracking_method`: INT,
          `fuel_consumed_milli_l`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
