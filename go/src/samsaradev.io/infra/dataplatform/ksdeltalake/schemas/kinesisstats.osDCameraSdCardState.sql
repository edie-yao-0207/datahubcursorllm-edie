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
    `camera_sd_card_state`: STRUCT<
      `state`: INT,
      `failure_reason`: STRING,
      `card_health`: STRUCT<
        `has_avg_erase_count`: BOOLEAN,
        `avg_erase_count`: BIGINT,
        `has_max_erase_count`: BOOLEAN,
        `max_erase_count`: BIGINT,
        `has_spare_block_count`: BOOLEAN,
        `spare_block_count`: BIGINT,
        `has_bad_block_count`: BOOLEAN,
        `bad_block_count`: BIGINT,
        `power_on_count`: BIGINT,
        `write_speed_mbps`: BIGINT,
        `read_speed_mbps`: BIGINT
      >,
      `sd_card_cid`: STRING,
      `total_storage_kb`: BIGINT,
      `health_state`: INT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
