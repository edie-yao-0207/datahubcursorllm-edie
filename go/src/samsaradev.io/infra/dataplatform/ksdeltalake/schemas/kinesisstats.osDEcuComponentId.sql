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
    `ecu_component_id`: ARRAY<
      STRUCT<
        `ecu_id`: BIGINT,
        `make`: STRING,
        `model`: STRING,
        `serial_number`: STRING,
        `unit_number`: STRING,
        `has_unit_number`: BOOLEAN,
        `protocol_source`: INT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
