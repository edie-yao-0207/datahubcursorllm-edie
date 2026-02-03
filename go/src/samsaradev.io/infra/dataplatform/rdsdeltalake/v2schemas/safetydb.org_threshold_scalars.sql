`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`harsh_accel_threshold_scalar` DOUBLE,
`harsh_brake_threshold_scalar` DOUBLE,
`harsh_turn_threshold_scalar` DOUBLE,
`use_advanced_scalars` BYTE,
`advanced_scalars` STRUCT<
  `harsh_accel_x_threshold_scalars`: STRUCT<
    `passenger`: FLOAT,
    `light_duty`: FLOAT,
    `heavy_duty`: FLOAT
  >,
  `harsh_brake_x_threshold_scalars`: STRUCT<
    `passenger`: FLOAT,
    `light_duty`: FLOAT,
    `heavy_duty`: FLOAT
  >,
  `harsh_turn_x_threshold_scalars`: STRUCT<
    `passenger`: FLOAT,
    `light_duty`: FLOAT,
    `heavy_duty`: FLOAT
  >
>,
`_raw_advanced_scalars` STRING,
`partition` STRING
