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
    `firmware_metrics`: STRUCT<
      `build`: STRING,
      `product_name`: STRING,
      `hardware_version`: STRING,
      `duration_ms`: DECIMAL(20, 0),
      `metrics`: ARRAY<
        STRUCT<
          `name`: STRING,
          `metric_type`: INT,
          `value`: FLOAT,
          `tags`: ARRAY<STRING>,
          `gauge_average_count`: BIGINT
        >
      >,
      `v2_metrics`: STRUCT<
        `counts_no_tags`: STRUCT<
          `names`: ARRAY<INT>,
          `values`: ARRAY<BIGINT>,
          `tags`: ARRAY<INT>
        >,
        `counts_one_tag`: STRUCT<
          `names`: ARRAY<INT>,
          `values`: ARRAY<BIGINT>,
          `tags`: ARRAY<INT>
        >,
        `counts_two_tags`: STRUCT<
          `names`: ARRAY<INT>,
          `values`: ARRAY<BIGINT>,
          `tags`: ARRAY<INT>
        >,
        `counts_fallback`: ARRAY<
          STRUCT<
            `name_enum`: INT,
            `name_string`: STRING,
            `value`: BIGINT,
            `tag_enums`: ARRAY<INT>,
            `tag_strings`: ARRAY<STRING>
          >
        >,
        `gauges_no_tags`: STRUCT<
          `names`: ARRAY<INT>,
          `values`: ARRAY<FLOAT>,
          `sample_count`: ARRAY<BIGINT>,
          `tags`: ARRAY<INT>
        >,
        `gauges_one_tag`: STRUCT<
          `names`: ARRAY<INT>,
          `values`: ARRAY<FLOAT>,
          `sample_count`: ARRAY<BIGINT>,
          `tags`: ARRAY<INT>
        >,
        `gauges_two_tags`: STRUCT<
          `names`: ARRAY<INT>,
          `values`: ARRAY<FLOAT>,
          `sample_count`: ARRAY<BIGINT>,
          `tags`: ARRAY<INT>
        >,
        `gauges_fallback`: ARRAY<
          STRUCT<
            `name_enum`: INT,
            `name_string`: STRING,
            `value`: FLOAT,
            `sample_count`: BIGINT,
            `tag_enums`: ARRAY<INT>,
            `tag_strings`: ARRAY<STRING>
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
