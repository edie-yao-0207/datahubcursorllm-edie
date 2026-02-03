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
    `widget_connection_life_cycle`: STRUCT<
      `gateway_id`: BIGINT,
      `in_queue`: STRUCT<
        `duration_ms`: BIGINT,
        `starting_rssi_dbm`: INT,
        `starting_queue_length`: BIGINT
      >,
      `trying_to_connect`: STRUCT<
        `duration_ms`: BIGINT,
        `starting_rssi_dbm`: INT,
        `starting_queue_length`: BIGINT
      >,
      `encryption_enabled`: BOOLEAN,
      `trying_to_encrypt`: STRUCT<
        `duration_ms`: BIGINT,
        `starting_rssi_dbm`: INT,
        `starting_queue_length`: BIGINT
      >,
      `connected`: STRUCT<
        `duration_ms`: BIGINT,
        `starting_rssi_dbm`: INT,
        `starting_queue_length`: BIGINT
      >,
      `waiting_for_set_seq`: STRUCT<
        `duration_ms`: BIGINT,
        `starting_rssi_dbm`: INT,
        `starting_queue_length`: BIGINT
      >,
      `time_since_first_adv_ms`: BIGINT,
      `catching_up`: STRUCT<
        `duration_ms`: BIGINT,
        `starting_rssi_dbm`: INT,
        `starting_queue_length`: BIGINT
      >,
      `caught_up_info`: STRUCT<
        `num_logs_until_caught_up`: BIGINT,
        `caught_up_via_set_seq`: BOOLEAN
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
