`request_id` STRING,
`status_code` BIGINT,
`path_template` STRING,
`http_method` STRING,
`duration_ms` BIGINT,
`ip_address` STRING,
`host` STRING,
`timestamp` TIMESTAMP,
`request_headers` MAP<
  STRING,
  ARRAY<STRING>
>,
`response_headers` MAP<
  STRING,
  ARRAY<STRING>
>,
`query_params` MAP<
  STRING,
  ARRAY<STRING>
>,
`body` BINARY,
`request_body` BINARY,
`body_length` BIGINT,
`request_body_length` BIGINT,
`ecs_task_id` STRING,
`url` STRING,
`date` DATE
