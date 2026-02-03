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
    `plc4trucks_research_data`: STRUCT<
      `tx_id`: BIGINT,
      `signal_id`: INT,
      `has_vehicle_message`: BOOLEAN,
      `vehicle_message`: BINARY
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
