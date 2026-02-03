SELECT
  rl.date,
  rl.timestamp,
  rl.org_id,
  rl.name,
  rl.user_id,
  rl.email,
  rl.duration_ms AS rl_duration_ms,
  rl.raw_url,
  rl.resource,
  rl.team_owner AS owner,
  rl.slo_grouping,
  gql.query_name,
  gql.protocol,
  gql.duration_ms AS gql_duration_ms
FROM
  perf_infra.org_route_loads rl
  INNER JOIN datastreams.gql_query_load gql ON rl.date = gql.date
  AND rl.trace_id = gql.route_load_trace_id
  AND gql.timestamp >= (rl.timestamp - INTERVAL 1 MINUTES)
  AND gql.timestamp <= (rl.timestamp + INTERVAL 1 MINUTES)
WHERE
  gql.gql_error_code = ''
  AND gql.duration_ms < 60000
