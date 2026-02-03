`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`name` STRING,
`output_type` BIGINT,
`device_id` BIGINT,
`machine_input_id` BIGINT,
`config` STRUCT<
  `output_type`: INT,
  `slave_pin_config`: STRUCT<
    `id`: BIGINT,
    `modbus_settings`: STRUCT<
      `unchanged_bits_mask`: BIGINT,
      `use_value_as_bool_for_all_bits`: BOOLEAN
    >
  >,
  `local_variable_config`: STRUCT<`id`: BIGINT>,
  `gateway_channel_analog_config`: STRUCT<
    `id`: BIGINT,
    `mode`: INT,
    `module_id`: BIGINT
  >,
  `gateway_channel_digital_config`: STRUCT<
    `id`: BIGINT,
    `module_id`: BIGINT
  >,
  `uuid`: STRING,
  `rest_api_config`: STRUCT<
    `id`: BIGINT,
    `url`: STRING,
    `variable_key`: STRING,
    `device_id`: BIGINT
  >,
  `label_value_mappings`: ARRAY<
    STRUCT<
      `label`: STRING,
      `value`: DOUBLE
    >
  >,
  `widget_id`: BIGINT
>,
`_raw_config` STRING,
`created_by` BIGINT,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`deleted` BYTE,
`asset_id` STRING,
`type_id` BIGINT,
`is_template` BYTE,
`template_data_output_uuid` STRING,
`data_input_data_group_id` BIGINT,
`partition` STRING
