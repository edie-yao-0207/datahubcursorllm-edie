`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`parent_id` STRING,
`name` STRING,
`asset_config` STRUCT<
  `custom_metadata`: STRUCT<
    `entries`: ARRAY<
      STRUCT<
        `label`: STRING,
        `value`: STRING
      >
    >
  >,
  `location_config`: STRUCT<
    `location_type`: INT,
    `point`: STRUCT<
      `lat`: FLOAT,
      `lng`: FLOAT
    >,
    `address`: STRUCT<
      `address`: STRING,
      `coordinates`: STRUCT<
        `lat`: FLOAT,
        `lng`: FLOAT
      >
    >,
    `data_input_id`: BIGINT,
    `data_group_id`: BIGINT
  >,
  `running_status_data_input_id`: BIGINT,
  `running_status_data_group_id`: BIGINT,
  `device_config`: STRUCT<
    `device_type_product_id`: DECIMAL(20, 0),
    `device_id`: DECIMAL(20, 0)
  >,
  `notes`: STRING
>,
`_raw_asset_config` STRING,
`created_at` TIMESTAMP,
`created_by` INT,
`updated_at` TIMESTAMP,
`updated_by` INT,
`deleted` BYTE,
`deleted_at` TIMESTAMP,
`is_template` BYTE,
`template_asset_uuid` STRING,
`device_id` BIGINT,
`supported_data_sources` INT,
`default_template_version` INT,
`partition` STRING
