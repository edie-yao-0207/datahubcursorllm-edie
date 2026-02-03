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
    `battery_info`: STRUCT<
      `cell`: ARRAY<
        STRUCT<
          `mv`: INT,
          `id`: INT
        >
      >,
      `temperature_mc`: INT,
      `charger`: STRUCT<
        `hardware`: INT,
        `status`: INT,
        `reg`: ARRAY<
          STRUCT<
            `address`: BIGINT,
            `value`: BIGINT
          >
        >
      >,
      `fuel_gauge`: STRUCT<
        `hardware`: INT,
        `reg`: ARRAY<
          STRUCT<
            `address`: BIGINT,
            `value`: BIGINT
          >
        >,
        `soc`: FLOAT,
        `mv`: FLOAT,
        `ma`: FLOAT
      >,
      `battery_level_e`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
