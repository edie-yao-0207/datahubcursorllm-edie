`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`way_id` BIGINT,
`day_of_week` BYTE,
`hour_of_day` BYTE,
`speed_distribution_json` STRUCT<
  `way_id`: BIGINT,
  `metadata`: STRUCT<
    `bin_size`: BIGINT,
    `unit`: STRING,
    `lower_bound`: BIGINT,
    `upper_bound`: BIGINT,
    `total_samples`: BIGINT
  >,
  `probabilities`: ARRAY<DOUBLE>
>,
`_raw_speed_distribution_json` STRING,
`partition` STRING
