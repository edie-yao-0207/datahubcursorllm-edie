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
    `q_r_code_scan_debug`: STRUCT<
      `scan_status`: INT,
      `points`: ARRAY<
        STRUCT<
          `x_frac`: FLOAT,
          `y_frac`: FLOAT
        >
      >,
      `data`: BINARY,
      `code`: STRUCT<
        `magic`: INT,
        `version_id`: BIGINT,
        `driver_assignment_lookup_key`: STRING
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
