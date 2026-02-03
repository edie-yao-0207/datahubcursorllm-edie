`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`oem_type` BIGINT,
`source_id` STRING,
`status` BIGINT,
`updated_at` TIMESTAMP,
`metadata` STRUCT<
  `navistar`: STRUCT<
    `pending_enrollment_correlation_id`: STRING,
    `pending_unenrollment_correlation_id`: STRING
  >,
  `gm`: STRUCT<
    `pending_enrollment_url`: STRING,
    `activated_data_services`: ARRAY<STRING>,
    `activation_requested_at_ms`: BIGINT
  >,
  `stellantis`: STRUCT<
    `id`: STRING,
    `expires_at_ms`: DECIMAL(20, 0)
  >,
  `error`: STRUCT<
    `oem_error_code`: STRING,
    `oem_error_message`: STRING,
    `http_error_code`: STRING,
    `http_error_message`: STRING
  >,
  `rivian`: STRUCT<`subscription_id`: STRING>
>,
`_raw_metadata` STRING,
`partition` STRING
