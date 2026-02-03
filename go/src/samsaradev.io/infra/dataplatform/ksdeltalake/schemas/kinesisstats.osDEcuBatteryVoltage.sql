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
    `ecu_battery_voltage`: ARRAY<
      STRUCT<
        `voltage_milliv`: INT,
        `protocol_source`: INT,
        `ecu_id`: INT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
