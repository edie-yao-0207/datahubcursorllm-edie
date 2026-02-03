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
    `bendix_stats`: STRUCT<
      `stats_capture_duration_ms`: BIGINT,
      `total_received_packets`: BIGINT,
      `total_dropped_packets`: BIGINT,
      `total_dropped_bytes`: BIGINT,
      `data_ingress_msgs_received`: BIGINT,
      `data_ingress_seqs_mismatched`: BIGINT,
      `data_ingress_blobs_completed`: BIGINT,
      `data_egress_msgs_sent`: BIGINT,
      `data_egress_msgs_retried`: BIGINT,
      `data_egress_blobs_retried`: BIGINT,
      `data_egress_blobs_dropped`: BIGINT,
      `data_egress_blobs_completed`: BIGINT,
      `total_checksum_mismatched_packets`: BIGINT,
      `device_resets_detected`: BIGINT,
      `handshake_confirmation_packets_received`: BIGINT
    >
  >
>,
`_filename` STRING,
`_sort_key` STRING
