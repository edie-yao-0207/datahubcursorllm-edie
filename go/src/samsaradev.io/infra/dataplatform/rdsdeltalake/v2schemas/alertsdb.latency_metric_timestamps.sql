`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`org_id` BIGINT,
`alert_id` BIGINT,
`started_at` BIGINT,
`object_type` INT,
`object_id` BIGINT,
`object_ids` STRING,
`output_key` STRING,
`timestamp_proto` STRUCT<
  `first_datapoint_happened_at_ms`: BIGINT,
  `first_datapoint_received_at_ms`: BIGINT,
  `scheduling_complete_ms`: BIGINT,
  `processing_complete_ms`: BIGINT,
  `alertable_at_set_ms`: BIGINT,
  `last_email_notification_sent_ms`: BIGINT,
  `last_email_notification_delivered_ms`: BIGINT,
  `last_sms_notification_sent_ms`: BIGINT,
  `last_sms_notification_delivered_ms`: BIGINT,
  `last_webhook_notification_sent_ms`: BIGINT,
  `last_webhook_notification_delivered_ms`: BIGINT,
  `minimum_activity_duration_ms`: BIGINT,
  `composite_datapoint_timestamps`: ARRAY<
    STRUCT<
      `identifier`: STRING,
      `first_datapoint_happened_at_ms`: BIGINT,
      `first_datapoint_received_at_ms`: BIGINT
    >
  >
>,
`_raw_timestamp_proto` STRING,
`date` STRING
