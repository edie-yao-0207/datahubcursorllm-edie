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
    `scantool_detected_info`: STRUCT<
      `message_id_detected`: BIGINT,
      `scantool_detected_metadata`: STRUCT<
        `scantool_coexistence_method`: INT,
        `diagnostics_inactive_duration_ms`: BIGINT,
        `candidate_message_ids_to_detect`: ARRAY<BIGINT>
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
