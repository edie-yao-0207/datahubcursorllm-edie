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
    `reefer_alarm_state`: STRUCT<
      `thermoking_codes`: ARRAY<BIGINT>,
      `carrier_codes`: ARRAY<BIGINT>,
      `carrier_eu_codes`: ARRAY<STRING>,
      `supra_alarm_state`: STRUCT<`led_on`: BOOLEAN>
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
