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
    `vdp_signal_cache_snapshot`: STRUCT<
      `entries`: ARRAY<
        STRUCT<
          `source`: STRUCT<
            `bus`: INT,
            `ecu_id`: BIGINT,
            `request_id`: BIGINT,
            `data_id`: BIGINT
          >,
          `signals`: ARRAY<
            STRUCT<
              `signal`: STRUCT<
                `obd_value`: INT,
                `int_value`: BIGINT,
                `string_value`: STRING,
                `double_value`: DOUBLE,
                `positive_diff_count`: DECIMAL(20, 0),
                `negative_diff_count`: DECIMAL(20, 0),
                `first_int_value`: BIGINT,
                `min_int_value`: BIGINT,
                `min_count`: DECIMAL(20, 0),
                `max_int_value`: BIGINT,
                `max_count`: DECIMAL(20, 0),
                `avg_value`: DOUBLE,
                `stddev`: DOUBLE,
                `q1_value`: DOUBLE,
                `median_value`: DOUBLE,
                `q3_value`: DOUBLE
              >,
              `updated_ago_ms`: DECIMAL(20, 0),
              `count`: DECIMAL(20, 0)
            >
          >
        >
      >,
      `duration_ms`: DECIMAL(20, 0)
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
