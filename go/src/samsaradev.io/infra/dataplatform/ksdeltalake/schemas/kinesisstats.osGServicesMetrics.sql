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
    `services_metrics`: STRUCT<
      `services_metrics`: ARRAY<
        STRUCT<
          `metric_name`: INT,
          `metric_custom_name`: STRING,
          `metric_type`: INT,
          `tags_values`: ARRAY<
            STRUCT<
              `tags`: ARRAY<
                STRUCT<
                  `tag_key`: INT,
                  `tag_custom_key`: STRING,
                  `tag_int_value`: BIGINT,
                  `tag_custom_value`: STRING
                >
              >,
              `int_value`: BIGINT,
              `int_diff_value`: BIGINT,
              `float_value`: FLOAT,
              `float_diff_value`: FLOAT
            >
          >
        >
      >,
      `end_time_offset_ms`: BIGINT,
      `duration_ms`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
