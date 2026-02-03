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
    `plc_status`: ARRAY<
      STRUCT<
        `id`: BIGINT,
        `program_hash`: STRING,
        `active`: BOOLEAN,
        `program_uuid`: STRING
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
