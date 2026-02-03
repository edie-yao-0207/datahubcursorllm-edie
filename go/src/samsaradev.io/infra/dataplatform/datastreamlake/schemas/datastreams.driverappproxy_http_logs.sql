`timestamp` TIMESTAMP,
`org_id` BIGINT,
`driver_id` BIGINT,
`request_id` STRING,
`url` STRING,
`host` STRING,
`http_method` STRING,
`status_code` BIGINT,
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
`request_body` STRING,
`request_body_uri` STRING,
`response_body` STRING,
`response_body_uri` STRING,
`response_body_length` BIGINT,
`request_body_length` BIGINT,
`duration_ms` BIGINT,
`cell` STRING,
`ip_addr` STRING,
`extras` STRING,
`partial_body` BOOLEAN,
`date` DATE
