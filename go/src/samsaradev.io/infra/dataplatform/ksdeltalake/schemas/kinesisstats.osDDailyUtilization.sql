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
    `daily_utilization`: STRUCT<
      `use_case_id`: STRING,
      `utilized_ms`: DECIMAL(20, 0),
      `available_ms`: DECIMAL(20, 0),
      `date`: STRING,
      `last_event_ms`: DECIMAL(20, 0),
      `utilizations`: ARRAY<
        STRUCT<
          `use_case_config_id`: STRING,
          `use_case_id`: STRING,
          `utilized_ms`: DECIMAL(20, 0),
          `selected`: BOOLEAN,
          `last_event_ms`: DECIMAL(20, 0)
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
