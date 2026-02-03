`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`_s3_big_stat_file_created_at_ms` BIGINT,
`s3_proto_value` STRUCT<
  `tachograph_vu_download`: STRUCT<`vu_download`: BINARY>
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
  `proto_value`: STRUCT<
    `tachograph_vu_download`: STRUCT<
      `license_plate`: STRING,
      `start_date_unix_secs`: BIGINT,
      `end_date_unix_secs`: BIGINT,
      `generation`: BIGINT,
      `manufacturer_name`: STRING,
      `part_number`: STRING
    >
  >,
  `received_delta_seconds`: BIGINT
>
