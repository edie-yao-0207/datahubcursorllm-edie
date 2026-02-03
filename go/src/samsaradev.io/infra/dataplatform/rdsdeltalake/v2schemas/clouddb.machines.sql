`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`id` BIGINT,
`group_id` BIGINT,
`name` STRING,
`notes` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`pump_proto` STRING,
`lat` DOUBLE,
`lng` DOUBLE,
`config_proto` STRUCT<
  `class`: INT,
  `type`: INT,
  `custom_type`: STRING,
  `manufacturer`: STRING,
  `model`: STRING,
  `serial`: STRING,
  `year`: INT,
  `rpm`: DOUBLE,
  `hp`: DOUBLE,
  `amperage`: DOUBLE,
  `voltage`: DOUBLE,
  `kilowatts`: DOUBLE,
  `frequency`: INT,
  `vrms_on_threshold`: DOUBLE,
  `vrms_warning_threshold`: DOUBLE,
  `vrms_error_threshold`: DOUBLE,
  `custom_metadata`: STRUCT<
    `metadata_fields`: ARRAY<
      STRUCT<
        `label`: STRING,
        `value`: STRING
      >
    >
  >
>,
`_raw_config_proto` STRING,
`image_s3_location` STRING,
`input_proto` STRING,
`asset_uuid` STRING,
`partition` STRING
