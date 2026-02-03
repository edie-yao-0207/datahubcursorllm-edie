`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`created_at` BIGINT,
`author_id` BIGINT,
`detail_proto` STRUCT<
  `org_threshold_scalars`: STRUCT<
    `harsh_accel_x_threshold_scalar`: FLOAT,
    `harsh_brake_x_threshold_scalar`: FLOAT,
    `harsh_turn_x_threshold_scalar`: FLOAT,
    `use_advanced_scalars`: BOOLEAN,
    `advanced_scalars`: STRUCT<
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
    >
  >
>,
`_raw_detail_proto` STRING,
`date` STRING
