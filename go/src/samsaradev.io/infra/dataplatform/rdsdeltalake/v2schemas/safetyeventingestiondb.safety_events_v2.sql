`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`device_id` BIGINT,
`start_ms` BIGINT,
`end_ms` BIGINT,
`detection_label` SHORT,
`uuid` STRING,
`customer_facing_labels` STRUCT<
  `customer_facing_labels`: ARRAY<
    STRUCT<
      `label_type`: INT,
      `label_source`: INT,
      `last_added_ms`: BIGINT,
      `user_id`: BIGINT
    >
  >
>,
`_raw_customer_facing_labels` STRING,
`customer_launch_release_stage` SHORT,
`event_id` BIGINT,
`event_id_source` SHORT,
`driver_id` BIGINT,
`trip_start_ms` BIGINT,
`created_at_ms` BIGINT,
`updated_at_ms` BIGINT,
`event_metadata` STRING,
`date` STRING
