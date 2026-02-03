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
    `engine_engage_event`: STRUCT<
      `engage_state`: INT,
      `start_ms`: BIGINT,
      `duration_ms`: BIGINT,
      `consumption_info`: STRUCT<
        `fuel_consumed_ml`: DOUBLE,
        `gaseous_fuel_consumed_grams`: DOUBLE,
        `energy_consumed_kwh`: DOUBLE,
        `fuel_level_millipercent`: DECIMAL(20, 0)
      >,
      `event_uuid`: STRING,
      `context`: STRUCT<`next_engine_state`: INT>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
