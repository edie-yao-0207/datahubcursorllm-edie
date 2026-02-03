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
    `j1939_bus_stats`: STRUCT<
      `multi_frame_infos`: ARRAY<
        STRUCT<
          `pgn`: BIGINT,
          `source_address`: BIGINT,
          `total_number_of_bytes_expected`: DECIMAL(20, 0),
          `max_number_of_bytes_expected`: DECIMAL(20, 0),
          `min_number_of_bytes_expected`: DECIMAL(20, 0),
          `request_to_send_count`: DECIMAL(20, 0),
          `bam_count`: DECIMAL(20, 0),
          `received_dropped_count`: DECIMAL(20, 0),
          `received_success_count`: DECIMAL(20, 0)
        >
      >,
      `number_of_multi_frame_infos_not_logged`: BIGINT,
      `log_period_ms`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
