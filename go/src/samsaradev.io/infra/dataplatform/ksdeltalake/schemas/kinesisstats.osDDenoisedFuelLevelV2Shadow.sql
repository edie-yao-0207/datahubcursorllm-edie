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
    `denoised_fuel_level_v2`: STRUCT<
      `data`: STRUCT<
        `fuel_level_millipercent`: BIGINT,
        `has_fuel_level_millipercent`: BOOLEAN,
        `fuel_level_millipercent_stddev`: BIGINT,
        `has_fuel_level_millipercent_stddev`: BOOLEAN,
        `is_fuel_level_passthrough_unfiltered`: BOOLEAN,
        `is_fuel_level_bad_input_data`: BOOLEAN,
        `metadata`: STRUCT<
          `parameter_version`: STRING,
          `ml_model_version`: STRING,
          `mean_absolute_error`: FLOAT,
          `standard_deviation`: FLOAT
        >
      >,
      `source_data`: STRUCT<
        `fuel_level_millipercent`: BIGINT,
        `has_fuel_level_millipercent`: BOOLEAN,
        `fuel2_level_millipercent`: BIGINT,
        `has_fuel2_level_millipercent`: BOOLEAN,
        `fuel_consumed_ml`: BIGINT,
        `has_fuel_consumed_ml`: BOOLEAN,
        `speed_milliknots`: BIGINT,
        `has_speed_milliknots`: BOOLEAN,
        `dt_ms`: BIGINT,
        `has_dt_ms`: BOOLEAN,
        `fuel_consumed_ml_per_hour`: BIGINT,
        `has_fuel_consumed_ml_per_hour`: BOOLEAN,
        `is_start`: BOOLEAN,
        `is_end`: BOOLEAN,
        `engine_state`: INT
      >,
      `experiments`: ARRAY<
        STRUCT<
          `fuel_level_millipercent`: BIGINT,
          `has_fuel_level_millipercent`: BOOLEAN,
          `fuel_level_millipercent_stddev`: BIGINT,
          `has_fuel_level_millipercent_stddev`: BOOLEAN,
          `is_fuel_level_passthrough_unfiltered`: BOOLEAN,
          `is_fuel_level_bad_input_data`: BOOLEAN,
          `metadata`: STRUCT<
            `parameter_version`: STRING,
            `ml_model_version`: STRING,
            `mean_absolute_error`: FLOAT,
            `standard_deviation`: FLOAT
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
