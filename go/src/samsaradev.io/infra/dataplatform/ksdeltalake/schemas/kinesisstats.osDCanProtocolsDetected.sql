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
    `can_protocols_detected`: STRUCT<
      `bus_id`: INT,
      `can_protocol_detection_params`: STRUCT<
        `can_j1939_protocol_detection_method`: INT,
        `enable_requested_j1939_address_claim_protocol_detection`: INT,
        `broadcast_j1939_loop_detection_timeout_ms`: BIGINT,
        `can_passenger_detection_method`: INT,
        `passenger_probe_send_attempts`: BIGINT,
        `reverse_passenger_protocol_detect_order`: INT,
        `allow_preferred_protocol_retry`: INT,
        `j1939_detection_on_all_buses_enabled`: INT,
        `broadcast_pgn_allowlist_enabled`: INT
      >,
      `detected_can_protocols`: ARRAY<
        STRUCT<
          `protocol`: INT,
          `protocol_detection_method`: INT,
          `selected_to_run`: BOOLEAN,
          `can_id_type`: INT,
          `default_module_address`: BIGINT,
          `protocol_detect_retry_count`: BIGINT,
          `broadcast_pgn_allowlist_enabled`: BOOLEAN,
          `j1939_can_ids_seen`: ARRAY<BIGINT>
        >
      >
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
