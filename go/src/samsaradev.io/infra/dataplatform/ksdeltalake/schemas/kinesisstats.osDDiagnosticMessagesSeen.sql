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
    `diagnostic_messages_seen`: ARRAY<
      STRUCT<
        `msg_id`: BIGINT,
        `txid`: BIGINT,
        `bus_type`: BIGINT,
        `last_bytes`: BINARY,
        `last_bytes_valid`: BOOLEAN,
        `last_reported_ago_ms`: BIGINT,
        `total_count`: BIGINT,
        `msg_id_uint64`: DECIMAL(20, 0)
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
