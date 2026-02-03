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
    `obd_cable_mfg_identity`: STRUCT<
      `cable_type`: INT,
      `mfg_date`: STRUCT<
        `day`: BIGINT,
        `month`: BIGINT,
        `year`: BIGINT
      >,
      `hardware_id`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
