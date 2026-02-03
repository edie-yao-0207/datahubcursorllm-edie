SELECT
  date,
  timestamp,
  resource,
  'route_load' AS type,
  'route.load' AS name,
  org.name AS org_name,
  org_id,
  user_id,
  usr.email AS email,
  duration_ms,
  owner AS team_owner,
  raw_url
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


UNION ALL
  SELECT date,
  timestamp,
  resource,
  'user_interaction' AS type,
  regexp_extract(ui.name, '(\\w+$)') AS name,
  o.name AS org_name,
  ui.org_id AS org_id,
  user_id,
  usr.email AS email,
  duration_ms,
  rc.owner AS team_owner,
  raw_url
  FROM
    datastreams.cloud_app_user_interactions AS ui
    INNER JOIN perf_infra.elite_orgs AS o ON ui.org_id = o.org_id
    INNER JOIN perf_infra.route_config AS rc ON rc.path = ui.resource
    INNER JOIN clouddb.users usr ON ui.user_id = usr.id
  WHERE
    ui.org_id IN (
      SELECT
        org_id
      FROM
        perf_infra.elite_orgs
    )
    AND usr.email NOT LIKE '%@samsara.com' -- Ignore traffic from employees.
    AND duration_ms < 60000
