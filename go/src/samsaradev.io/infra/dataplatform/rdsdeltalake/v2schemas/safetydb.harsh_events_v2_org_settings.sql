`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`setting` STRUCT<
  `harsh_accel_sensitivity`: INT,
  `harsh_brake_sensitivity`: INT,
  `harsh_turn_sensitivity`: INT,
  `advanced_settings`: STRUCT<
    `harsh_accel`: STRUCT<
      `passenger`: INT,
      `light_duty`: INT,
      `heavy_duty`: INT
    >,
    `harsh_brake`: STRUCT<
      `passenger`: INT,
      `light_duty`: INT,
      `heavy_duty`: INT
    >,
    `harsh_turn`: STRUCT<
      `passenger`: INT,
      `light_duty`: INT,
      `heavy_duty`: INT
    >
  >
>,
`_raw_setting` STRING,
`partition` STRING
