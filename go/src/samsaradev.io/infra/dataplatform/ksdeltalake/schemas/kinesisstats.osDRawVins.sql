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
    `raw_vins`: STRUCT<
      `raw_vins`: ARRAY<STRING>,
      `raw_vin_infos`: ARRAY<
        STRUCT<
          `raw_vin`: STRING,
          `vehicle_diagnostic_bus`: INT,
          `tx_id`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
