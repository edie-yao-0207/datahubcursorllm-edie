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
    `fill_level_summary`: STRUCT<
      `primary`: STRUCT<
        `level`: STRUCT<
          `millipercent`: BIGINT,
          `mass_grams`: BIGINT,
          `volume_milliliters`: BIGINT
        >,
        `ingress`: STRUCT<
          `millipercent`: BIGINT,
          `mass_grams`: BIGINT,
          `volume_milliliters`: BIGINT
        >,
        `egress`: STRUCT<
          `millipercent`: BIGINT,
          `mass_grams`: BIGINT,
          `volume_milliliters`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
