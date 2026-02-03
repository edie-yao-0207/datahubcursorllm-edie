`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`uuid` STRING,
`planner_uuid` STRING,
`planner_session_uuid` STRING,
`created_at` TIMESTAMP,
`created_by` BIGINT,
`updated_by` BIGINT,
`updated_at` TIMESTAMP,
`server_deleted_at` TIMESTAMP,
`server_deleted_by` BIGINT,
`completed_at` TIMESTAMP,
`cost` DOUBLE,
`status` INT,
`error_message` STRING,
`async_job_id` STRING,
`status_history_proto` STRUCT<
  `statuses`: ARRAY<
    STRUCT<
      `status`: INT,
      `updated_at`: TIMESTAMP
    >
  >
>,
`_raw_status_history_proto` STRING,
`raw_error_message` STRING,
`is_server_error` BYTE,
`partition` STRING
