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
    `marathon_runtime_health_stats`: STRUCT<
      `soc_list`: ARRAY<
        STRUCT<
          `soc`: INT,
          `heap_stats`: STRUCT<
            `capacity_bytes`: BIGINT,
            `current_bytes_allocated`: BIGINT,
            `max_bytes_allocated`: BIGINT,
            `current_blocks_allocated`: BIGINT,
            `max_blocks_allocated`: BIGINT
          >,
          `sys_heap_stats`: STRUCT<
            `capacity_bytes`: BIGINT,
            `current_bytes_allocated`: BIGINT
          >,
          `total_thread_execution_cycles`: DECIMAL(20, 0),
          `thread_stats`: ARRAY<
            STRUCT<
              `id`: BIGINT,
              `name`: STRING,
              `runtime_percent`: FLOAT,
              `stack_size`: BIGINT,
              `max_stack_used`: BIGINT
            >
          >,
          `firmware_build`: BIGINT
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
