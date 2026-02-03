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
    `attached_usb_devices`: ARRAY<
      STRUCT<
        `usb_id`: BIGINT,
        `serial`: STRING,
        `port_topology`: STRING,
        `speed`: INT,
        `bcd_device`: BIGINT
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
