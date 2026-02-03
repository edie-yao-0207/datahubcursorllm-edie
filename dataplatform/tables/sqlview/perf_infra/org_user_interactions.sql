SELECT
  rc.owner AS team_owner,
  rc.sloGrouping AS slo_grouping,
  o.name AS org_name,
  regexp_extract(ui.name, '(\\w+$)') AS interaction_type,
  ui.*
FROM
  datastreams.cloud_app_user_interactions AS ui
  INNER JOIN clouddb.organizations AS o ON ui.org_id = o.id
  INNER JOIN clouddb.users usr ON ui.user_id = usr.id
  INNER JOIN perf_infra.route_config AS rc ON rc.path = ui.resource
  -- filter out any durations larger than 5m. these interactions appear in our
  -- data but are clear outliers and should not be presented alongside
  -- non-erroneous performance data
  WHERE ui.duration_ms < 300000
  AND o.internal_type != 1
  AND usr.email NOT LIKE '%@samsara.com'
