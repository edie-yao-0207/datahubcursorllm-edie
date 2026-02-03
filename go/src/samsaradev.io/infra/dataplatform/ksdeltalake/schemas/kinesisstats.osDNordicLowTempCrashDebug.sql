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
    `nordic_low_temp_crash_debug`: STRUCT<
      `boot_count`: BIGINT,
      `high_freq_patch`: STRUCT<
        `vote_mask`: BIGINT,
        `has_voted_mask`: BIGINT,
        `num_work_q_votes`: BIGINT,
        `num_zero_votes`: BIGINT,
        `last_zero_vote_ms`: BIGINT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
