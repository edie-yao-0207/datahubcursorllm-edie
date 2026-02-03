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
    `cellular_call_end_reasons`: STRUCT<
      `mdm9607`: STRUCT<
        `reasons`: ARRAY<
          STRUCT<
            `type`: INT,
            `mobile_ip_code`: INT,
            `internal_code`: INT,
            `call_manager_code`: INT,
            `three_gpp_code`: INT,
            `ppp_code`: INT,
            `ehrpd_code`: INT,
            `ipv6_code`: INT,
            `count`: BIGINT
          >
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
