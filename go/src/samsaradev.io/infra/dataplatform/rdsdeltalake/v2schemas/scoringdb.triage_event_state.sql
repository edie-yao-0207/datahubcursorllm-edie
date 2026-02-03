`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`triage_event_uuid` STRING,
`trip_start_ms` BIGINT,
`triage_event_data` STRUCT<
  `behaviors`: ARRAY<
    STRUCT<
      `behavior_label`: INT,
      `detection_method`: INT
    >
  >,
  `context_labels`: ARRAY<
    STRUCT<
      `label_id`: STRING,
      `detection_method`: INT
    >
  >,
  `start_ms`: BIGINT,
  `end_ms`: BIGINT,
  `trip_start_ms`: BIGINT,
  `triage_state`: INT
>,
`_raw_triage_event_data` STRING,
`date` STRING
