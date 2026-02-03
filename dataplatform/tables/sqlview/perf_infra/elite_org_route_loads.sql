SELECT
  date,
  timestamp,
  resource,
  org_id,
  org.name AS name,
  user_id,
  usr.email AS email,
  duration_ms,
  raw_url,
  owner AS team_owner,
  sloGrouping AS slo_grouping,
  initial_load,
  http_protocol,
  h2_possible,
  trace_id
FROM
  datastreams.frontend_routeload frl
  INNER JOIN perf_infra.route_config rc ON frl.resource = rc.path
  INNER JOIN clouddb.organizations org ON frl.org_id = org.id
  INNER JOIN clouddb.users usr ON frl.user_id = usr.id
WHERE
  org_id IN (
    SELECT
      org_id
    FROM
      perf_infra.elite_orgs
  )
  AND usr.email NOT LIKE '%@samsara.com' -- Ignore traffic from employees.
  AND had_error = false
  AND duration_ms < 60000
