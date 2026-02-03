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
    `marathon_jotter_stats`: STRUCT<
      `soc_list`: ARRAY<
        STRUCT<
          `soc`: INT,
          `jotter_stats`: ARRAY<
            STRUCT<
              `category`: INT,
              `ram_cache_usage`: STRUCT<
                `capacity_bytes`: BIGINT,
                `current_bytes_allocated`: BIGINT,
                `max_bytes_allocated`: BIGINT,
                `current_blocks_allocated`: BIGINT,
                `max_blocks_allocated`: BIGINT
              >,
              `flash_usage`: STRUCT<
                `current_bytes_used`: BIGINT,
                `max_bytes_used`: BIGINT,
                `capacity_bytes`: BIGINT
              >
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
