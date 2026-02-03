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
    `j1939_probe_results`: ARRAY<
      STRUCT<
        `can_bus_type`: BIGINT,
        `all_frames_equal`: BOOLEAN,
        `num_matching_pgns`: BIGINT,
        `all_frames_equal_pgn`: BIGINT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
