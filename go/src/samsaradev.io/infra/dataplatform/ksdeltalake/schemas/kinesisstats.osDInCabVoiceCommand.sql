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
    `in_cab_voice_command`: STRUCT<
      `command`: INT,
      `command_confidence`: FLOAT,
      `command_phrase`: STRING,
      `wake_word_phrase`: STRING,
      `wake_word_confidence`: FLOAT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
