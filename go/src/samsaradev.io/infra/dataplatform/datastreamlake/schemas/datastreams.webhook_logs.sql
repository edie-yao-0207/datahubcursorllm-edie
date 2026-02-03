`org_id` BIGINT,
`request_id` STRING,
`timestamp` TIMESTAMP,
`status_code` BIGINT,
`webhook_id` BIGINT,
`payload_type_id` BIGINT,
`payload_type_name` STRING,
`request_headers` MAP<
  STRING,
  ARRAY<STRING>
>,
`response_headers` MAP<
  STRING,
  ARRAY<STRING>
>,
`body` BINARY,
`response_body` BINARY,
`body_length` BIGINT,
`response_body_length` BIGINT,
`alert_condition_description` STRING,
`alert_event_url` STRING,
`alert_details` STRING,
`alert_summary` STRING,
`url` STRING,
`date` DATE
