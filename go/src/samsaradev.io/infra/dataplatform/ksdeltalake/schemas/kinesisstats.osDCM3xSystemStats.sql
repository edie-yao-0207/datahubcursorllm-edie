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
    `cm3x_system_stats`: STRUCT<
      `total_cpu_util`: FLOAT,
      `cpu_stats`: ARRAY<
        STRUCT<
          `cpu_name`: STRING,
          `cpu_util`: FLOAT,
          `min_freq`: BIGINT,
          `max_freq`: BIGINT,
          `cur_freq`: BIGINT,
          `online`: BOOLEAN
        >
      >,
      `memory_util`: FLOAT,
      `gpu_util`: FLOAT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
