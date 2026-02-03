`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`created_by` BIGINT,
`created_at_ms` BIGINT,
`updated_at_ms` BIGINT,
`start_ms` BIGINT,
`duration_ms` BIGINT,
`report_name` STRING,
`report_definition` STRUCT<
  `columns`: ARRAY<
    STRUCT<
      `id`: BIGINT,
      `display_name`: STRING,
      `string_id`: STRING,
      `data_type`: INT,
      `unit`: STRUCT<
        `unit_type`: INT,
        `base_unit`: INT
      >,
      `numeric_display_precision`: INT
    >
  >,
  `tag_ids`: ARRAY<BIGINT>,
  `sort_by_column_id`: BIGINT,
  `sort_ascending`: BOOLEAN,
  `entity_type`: BIGINT,
  `attributes`: STRUCT<
    `attributes_query`: ARRAY<
      STRUCT<
        `attribute_id`: STRING,
        `attribute_value_id`: STRING,
        `operation`: STRUCT<
          `operator`: INT,
          `value`: DOUBLE
        >,
        `attribute_type`: INT,
        `secondary_ids`: STRUCT<
          `attribute_name`: STRING,
          `attribute_value_name`: STRING,
          `entity_type`: INT
        >,
        `filter_range`: STRUCT<
          `minimum`: DOUBLE,
          `maximum`: DOUBLE,
          `min_date_ms`: BIGINT,
          `max_date_ms`: BIGINT
        >
      >
    >
  >
>,
`_raw_report_definition` STRING,
`deleted_at_ms` BIGINT,
`is_superuser_only` BYTE,
`partition` STRING
