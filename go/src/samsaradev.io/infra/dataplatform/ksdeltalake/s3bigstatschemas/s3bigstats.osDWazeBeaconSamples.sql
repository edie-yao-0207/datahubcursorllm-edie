`date` STRING,
`stat_type` INT,
`org_id` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`time` BIGINT,
`_s3_big_stat_file_created_at_ms` BIGINT,
`s3_proto_value` STRUCT<
  `waze_beacon_samples`: ARRAY<
    STRUCT<
      `instance_id`: DECIMAL(20, 0),
      `rssi_dbm`: INT,
      `event_offset_ms`: BIGINT
    >
  >
>,
`_synced_at` TIMESTAMP
