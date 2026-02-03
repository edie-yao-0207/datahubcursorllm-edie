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
    `tachograph_company_card_info`: STRUCT<
      `card_number`: STRING,
      `issuing_authority_name`: STRING,
      `company_name`: STRING,
      `issue_date_epoch_seconds`: BIGINT,
      `valid_from_epoch_seconds`: BIGINT,
      `expiry_date_epoch_seconds`: BIGINT,
      `card_serial_number`: STRING
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
