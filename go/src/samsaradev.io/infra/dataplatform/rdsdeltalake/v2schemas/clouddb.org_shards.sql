`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`shard_type` INT,
`data_type` STRING,
`shard_name` STRING,
`read_only` BYTE,
`status_proto` STRUCT<
  `kinesis_stats`: STRUCT<
    `state`: INT,
    `secondary_cluster_id`: STRING,
    `updated_at_ms`: BIGINT,
    `orgmover_checkpoint`: MAP<STRING, STRING>,
    `orgmover_shard_series`: MAP<STRING, STRING>,
    `batch_job_id`: STRING,
    `wait_duration_ms`: BIGINT,
    `orgmover_writes_start_ms`: BIGINT,
    `source_cluster_id`: STRING
  >,
  `read_org_shard_from_cloud_db`: BOOLEAN,
  `db_migration_status`: STRUCT<
    `source_shard_name`: STRING,
    `target_shard_name`: STRING
  >
>,
`_raw_status_proto` STRING,
`partition` STRING
