SELECT
  ui.date,
  ui.timestamp,
  ui.org_id,
  cdbo.name AS name,
  ui.name AS ui_name,
  ui.user_id,
  cdbu.email,
  ui.duration_ms AS ui_duration_ms,
  ui.resource,
  ui.team_owner AS owner,
  ui.slo_grouping,
  gql.query_name,
  gql.protocol,
  gql.duration_ms AS gql_duration_ms
FROM
  perf_infra.elite_org_user_interactions ui
  INNER JOIN datastreams.gql_query_load gql ON ui.date = gql.date
  INNER JOIN clouddb.users cdbu ON cdbu.id = ui.user_id
  INNER JOIN clouddb.organizations cdbo ON cdbo.id = ui.org_id
  AND ui.trace_id = gql.route_load_trace_id
  AND gql.timestamp >= (ui.timestamp - INTERVAL 1 MINUTES)
  AND gql.timestamp <= (ui.timestamp + INTERVAL 1 MINUTES)
WHERE
  gql.gql_error_code = ''
  AND gql.duration_ms < 60000
