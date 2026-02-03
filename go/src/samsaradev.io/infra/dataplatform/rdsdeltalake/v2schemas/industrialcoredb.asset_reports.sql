`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`name` STRING,
`config` STRUCT<
  `asset_uuids`: ARRAY<BINARY>,
  `column_configs`: ARRAY<
    STRUCT<
      `data_input_type`: STRUCT<
        `id`: BIGINT,
        `summary_function`: INT,
        `name_for_generation`: STRING
      >,
      `asset_metadata`: STRUCT<`label`: STRING>,
      `asset_location`: STRUCT<`type`: INT>,
      `custom_filters`: ARRAY<
        STRUCT<
          `type`: INT,
          `number_filter`: STRUCT<
            `value`: DOUBLE,
            `comparator`: INT
          >,
          `string_filter`: STRUCT<
            `pattern`: STRING,
            `exact_match`: BOOLEAN
          >
        >
      >,
      `sort_by`: INT,
      `alert_id`: BIGINT,
      `custom_name`: STRING,
      `reserved_metadata`: STRUCT<`type`: INT>,
      `hidden`: BOOLEAN,
      `generated`: BOOLEAN
    >
  >,
  `swap_axes`: BOOLEAN,
  `interval_ms`: BIGINT,
  `asset_label_uuids`: ARRAY<BINARY>,
  `asset_label_key_uuids`: ARRAY<BINARY>,
  `sort_by_asset_name`: INT,
  `sort_by_end_time`: INT,
  `asset_job_uuids`: ARRAY<BINARY>,
  `asset_filter_type`: INT
>,
`_raw_config` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`deleted` BYTE,
`dashboard_id` BIGINT,
`dynamic_dashboard_uuid` STRING,
`is_featured` BYTE,
`partition` STRING
