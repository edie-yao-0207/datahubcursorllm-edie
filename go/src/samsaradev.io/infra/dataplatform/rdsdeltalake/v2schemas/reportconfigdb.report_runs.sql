`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`user_id` BIGINT,
`custom_report_uuid` STRING,
`start_ms` BIGINT,
`duration_ms` BIGINT,
`status` INT,
`percent_complete` INT,
`report_run_metadata` STRUCT<
  `report_definition`: STRUCT<
    `report_name`: STRING,
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
    `entity_type`: BIGINT,
    `tag_ids`: ARRAY<BIGINT>,
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
  `report_config`: STRUCT<
    `org_id`: BIGINT,
    `report_id`: STRING,
    `report_name`: STRING,
    `owner_id`: BIGINT,
    `created_at`: BIGINT,
    `updated_at`: BIGINT,
    `deleted_at`: BIGINT,
    `created_from`: STRUCT<
      `id`: STRING,
      `source`: INT
    >,
    `data_set`: STRING,
    `columns`: ARRAY<
      STRUCT<
        `column_id`: STRING,
        `field_name`: STRING,
        `column_name`: STRING,
        `display_name`: STRING,
        `numeric_precision`: BIGINT,
        `datetime_format`: STRING,
        `aggregation`: INT,
        `unit`: STRUCT<
          `unit_type`: INT,
          `base_unit`: INT
        >,
        `time_range_interval_boundary`: INT,
        `hidden`: BOOLEAN,
        `width`: INT,
        `requires_external_data_source_integration`: BOOLEAN,
        `formula`: STRING,
        `column_source`: INT
      >
    >,
    `filters`: ARRAY<
      STRUCT<
        `field_name`: STRING,
        `filter_value`: STRUCT<
          `values`: ARRAY<
            STRUCT<
              `null`: BOOLEAN,
              `integer`: BIGINT,
              `float`: DOUBLE,
              `timestamp_ms`: BIGINT,
              `string`: STRING,
              `masked`: BOOLEAN,
              `string_values`: ARRAY<STRING>
            >
          >,
          `operator`: INT
        >,
        `is_hidden`: BOOLEAN,
        `is_editable`: BOOLEAN
      >
    >,
    `summary_metrics`: ARRAY<
      STRUCT<`id`: STRING>
    >,
    `visualizations`: ARRAY<
      STRUCT<
        `id`: STRING,
        `title`: STRING,
        `subtitle`: STRING,
        `type`: INT,
        `x_axis`: STRUCT<
          `column_id`: STRING,
          `label`: STRING,
          `interval`: INT,
          `sort_option`: STRUCT<
            `sort_column_id`: STRING,
            `sort_field_name`: STRING,
            `sort_direction`: INT
          >,
          `aggregation`: INT
        >,
        `y_axes`: ARRAY<
          STRUCT<
            `columns`: ARRAY<
              STRUCT<
                `column_id`: STRING,
                `aggregation`: INT,
                `group_by`: STRING
              >
            >,
            `label`: STRING
          >
        >,
        `chart_settings`: STRUCT<
          `bar_chart_settings`: STRUCT<`group_type`: INT>,
          `geo_chart_settings`: STRUCT<
            `latitude_column_id`: STRING,
            `longitude_column_id`: STRING,
            `address_column_id`: STRING
          >,
          `heatmap_settings`: STRUCT<
            `y_axis_column_id`: STRING,
            `value_column_id`: STRING,
            `value_aggregation`: INT
          >,
          `scatter_chart_settings`: STRUCT<`show_regression_lines`: BOOLEAN>
        >,
        `breakdown_by`: STRING
      >
    >,
    `is_superuser_only`: BOOLEAN,
    `group_by_selections`: ARRAY<
      STRUCT<
        `field_name`: STRING,
        `configuration`: STRUCT<
          `time_interval`: BIGINT,
          `interval`: INT
        >
      >
    >,
    `sort_options`: ARRAY<
      STRUCT<
        `sort_column_id`: STRING,
        `sort_field_name`: STRING,
        `sort_direction`: INT
      >
    >,
    `timezone`: STRING,
    `breakdown_by`: INT,
    `unit_system`: STRING
  >,
  `run_options`: STRUCT<
    `filters`: ARRAY<
      STRUCT<
        `field_name`: STRING,
        `filter_value`: STRUCT<
          `values`: ARRAY<
            STRUCT<
              `null`: BOOLEAN,
              `integer`: BIGINT,
              `float`: DOUBLE,
              `timestamp_ms`: BIGINT,
              `string`: STRING,
              `masked`: BOOLEAN,
              `string_values`: ARRAY<STRING>
            >
          >,
          `operator`: INT
        >
      >
    >
  >,
  `report_row_count`: BIGINT,
  `language`: STRING,
  `applied_retention_data_types`: ARRAY<
    STRUCT<
      `source`: STRING,
      `applied_retention_data_type`: INT
    >
  >,
  `cached_from_run_uuid`: BINARY,
  `preview_entities_metadata`: STRUCT<
    `heuristic_name`: STRING,
    `entity_limit`: DECIMAL(20, 0)
  >,
  `authz_pushdown_statuses`: ARRAY<
    STRUCT<
      `table_name`: STRING,
      `pushed_down_columns`: ARRAY<STRING>,
      `not_pushed_down_columns`: ARRAY<STRING>
    >
  >
>,
`_raw_report_run_metadata` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`is_legacy` BYTE,
`custom_report_config` STRING,
`primary_data_view_id` STRING,
`is_ephemeral` BYTE,
`source` STRING,
`source_id` STRING,
`storage_type` INT,
`sub_status` INT,
`should_send_notification` BYTE,
`run_mode` INT,
`date` STRING
