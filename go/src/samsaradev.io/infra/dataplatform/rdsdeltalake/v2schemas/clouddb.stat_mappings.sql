`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`id` BIGINT,
`machine_input_id` BIGINT,
`object_id` BIGINT,
`object_type` SHORT,
`stat_type` SHORT,
`start_ms` BIGINT,
`end_ms` BIGINT,
`input_min` BIGINT,
`input_max` BIGINT,
`scale_min` BIGINT,
`scale_max` BIGINT,
`label_off` STRING,
`label_on` STRING,
`created_by` BIGINT,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`proto` STRUCT<
  `scale`: STRUCT<
    `no_scale`: BOOLEAN,
    `min_max_scale`: STRUCT<
      `input_min`: BIGINT,
      `input_max`: BIGINT,
      `scale_min`: BIGINT,
      `scale_max`: BIGINT
    >,
    `discrete_scale`: STRUCT<
      `label_on`: STRING,
      `label_off`: STRING
    >,
    `counter_scale`: STRUCT<`value_per_pulse`: BIGINT>
  >,
  `formula`: STRING,
  `formula_active`: BOOLEAN,
  `variables`: ARRAY<
    STRUCT<
      `variable`: STRING,
      `machineInputId`: BIGINT,
      `data_group_id`: BIGINT
    >
  >,
  `validation_config`: STRUCT<
    `data_type`: INT,
    `validation_formula`: STRING,
    `enum_options`: ARRAY<STRING>,
    `event_input`: BOOLEAN,
    `previous_ref`: BOOLEAN
  >,
  `summary_configuration`: STRUCT<
    `summary_active`: BOOLEAN,
    `summary_function`: INT,
    `interval_ms`: BIGINT,
    `downtime_trigger_ms`: BIGINT
  >,
  `discrete_config`: STRUCT<
    `discrete_cutoffs`: ARRAY<
      STRUCT<
        `cutoff`: DOUBLE,
        `value`: DOUBLE,
        `label`: STRING
      >
    >,
    `default_value`: DOUBLE,
    `default_label`: STRING
  >,
  `moving_window_config`: STRUCT<
    `moving_window_function`: INT,
    `window_size_ms`: BIGINT
  >,
  `canbus_config`: STRUCT<
    `master_device_id`: BIGINT,
    `object_stat_type`: INT,
    `object_stat_field`: BIGINT
  >,
  `asset_aggregate_config`: STRUCT<
    `asset_aggregate_active`: BOOLEAN,
    `function`: INT,
    `sources`: STRUCT<
      `parent_asset_uuid`: STRING,
      `data_group_ids`: ARRAY<BIGINT>
    >,
    `ignore_nan`: BOOLEAN
  >,
  `device_diagnostic_config`: STRUCT<`diagnostic_type`: INT>
>,
`_raw_proto` STRING,
`partition` STRING
