`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`_s3_big_stat_file_created_at_ms` BIGINT,
`s3_proto_value` STRUCT<
  `accel_mag_samples_batch`: ARRAY<
    STRUCT<
      `offset_ms`: DECIMAL(20, 0),
      `mag_milligravities`: DECIMAL(20, 0)
    >
  >
>,
`_synced_at` TIMESTAMP,
`value` STRUCT<
  `date`: STRING,
  `time`: BIGINT,
  `is_start`: BOOLEAN,
  `is_end`: BOOLEAN,
  `is_databreak`: BOOLEAN,
  `int_value`: BIGINT,
  `double_value`: DOUBLE,
  `received_delta_seconds`: BIGINT
>
