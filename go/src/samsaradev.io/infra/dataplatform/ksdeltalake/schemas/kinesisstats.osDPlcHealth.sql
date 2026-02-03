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
    `plc_health`: STRUCT<
      `health_entry`: ARRAY<
        STRUCT<
          `entry_type`: INT,
          `accuracy`: STRUCT<
            `failure_count`: BIGINT,
            `total_count`: BIGINT
          >,
          `latency`: STRUCT<
            `p50`: FLOAT,
            `p90`: FLOAT,
            `p99`: FLOAT,
            `mean`: FLOAT
          >
        >
      >,
      `mean_scan_rate_hz`: BIGINT,
      `inter_scan_delay_metrics`: STRUCT<
        `mean_ns`: BIGINT,
        `longest_ns`: BIGINT
      >,
      `scan_duration_metrics`: STRUCT<
        `mean_ns`: BIGINT,
        `longest_ns`: BIGINT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
