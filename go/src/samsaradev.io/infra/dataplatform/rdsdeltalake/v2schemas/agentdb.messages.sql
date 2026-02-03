`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`message_id` STRING,
`session_id` STRING,
`content` STRUCT<
  `parts`: ARRAY<
    STRUCT<
      `type`: INT,
      `markdown`: STRING
    >
  >
>,
`_raw_content` STRING,
`created_by` STRING,
`created_at` TIMESTAMP,
`updated_at` TIMESTAMP,
`deleted_at` TIMESTAMP,
`date` STRING
