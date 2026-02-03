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
    `connectivity_recovery`: STRUCT<
      `state_on_connect`: STRUCT<
        `time_unix_ms`: BIGINT,
        `boot_count`: BIGINT,
        `sim_switching_enabled`: BOOLEAN,
        `active_sim_slot`: INT,
        `interface`: INT
      >,
      `state_on_disconnect`: STRUCT<
        `time_unix_ms`: BIGINT,
        `boot_count`: BIGINT,
        `sim_switching_enabled`: BOOLEAN,
        `active_sim_slot`: INT,
        `interface`: INT
      >,
      `state_on_reconnect`: STRUCT<
        `time_unix_ms`: BIGINT,
        `boot_count`: BIGINT,
        `sim_switching_enabled`: BOOLEAN,
        `active_sim_slot`: INT,
        `interface`: INT
      >,
      `num_data_call_restarts_attempted`: BIGINT,
      `num_modem_resets_attempted`: BIGINT,
      `num_sim_switches_attempted`: BIGINT,
      `num_reboots_attempted`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
