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
    `engine_idle_event`: STRUCT<
      `idling_state`: INT,
      `start_ms`: BIGINT,
      `duration_ms`: BIGINT,
      `context`: STRUCT<
        `pto_state`: INT,
        `active_pto_inputs`: ARRAY<
          STRUCT<
            `source`: INT,
            `digio_input_type`: INT,
            `digio_port`: BIGINT
          >
        >,
        `location`: STRUCT<
          `latitude`: DOUBLE,
          `longitude`: DOUBLE,
          `house_number`: STRING,
          `street`: STRING,
          `neighborhood`: STRING,
          `city`: STRING,
          `state`: STRING,
          `country`: STRING,
          `post_code`: STRING
        >,
        `ambient_air_temperature_milli_celsius`: BIGINT,
        `diesel_regen_state`: INT,
        `next_engine_state`: INT,
        `min_idling_duration_device_config_ms`: BIGINT
      >,
      `consumption_info`: STRUCT<
        `fuel_consumed_ml`: DOUBLE,
        `gaseous_fuel_consumed_grams`: DOUBLE,
        `fuel_level_millipercent`: DECIMAL(20, 0)
      >,
      `event_uuid`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
