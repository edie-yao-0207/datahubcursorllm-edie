`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`value` STRUCT<
  `date`: STRING,
  `time`: BIGINT,
  `received_delta_seconds`: BIGINT,
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
  >
>,
`_filename` STRING,
`_sort_key` STRING
