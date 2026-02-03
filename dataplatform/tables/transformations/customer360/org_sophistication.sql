WITH org_num_routes AS (
  SELECT
    c.date,
    org_id,
    COUNT(DISTINCT r.id) as num_routes
  FROM dispatchdb_shards.shardable_dispatch_routes r
  LEFT JOIN definitions.445_calendar c
  ON
    c.date >= to_date(r.created_at) AND
    c.date <= from_unixtime(scheduled_start_ms / 1000)
  GROUP BY
    c.date,
    org_id
),

sam_num_routes AS (
  SELECT
    r.date,
    r.org_id,
    s.sam_number,
    sum(num_routes) AS num_routes
  FROM org_num_routes r
  LEFT JOIN clouddb.org_sfdc_accounts o ON r.org_id = o.org_id
  LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
  GROUP BY
    r.date,
    r.org_id,
    s.sam_number
),

org_num_users AS (
  SELECT
    c.date,
    organization_id as org_id,
    COUNT(DISTINCT u.id) as num_cloud_users
  FROM clouddb.users_organizations u
  LEFT JOIN definitions.445_calendar c
  ON
    c.date >= to_date(u.created_at) AND
    c.date <= current_date()
  GROUP BY
    c.date,
    org_id
),

sam_num_users AS (
  SELECT
    u.date,
    u.org_id,
    s.sam_number,
    sum(num_cloud_users) AS num_cloud_users
  FROM org_num_users u
  LEFT JOIN clouddb.org_sfdc_accounts o ON u.org_id = o.org_id
  LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
  GROUP BY
    u.date,
    u.org_id,
    s.sam_number
),

org_num_addresses AS (
  SELECT
    c.date,
    g.organization_id as org_id,
    COUNT(DISTINCT a.id) as num_addresses
  FROM clouddb.addresses a
  LEFT JOIN clouddb.groups g on a.group_id = g.id
  LEFT JOIN definitions.445_calendar c
  ON
    c.date >= to_date(a.created_at) AND
    c.date <= current_date()
  GROUP BY
    c.date,
    g.organization_id
),

sam_num_addresses AS (
  SELECT
    a.date,
    a.org_id,
    s.sam_number,
    SUM(num_addresses) AS num_addresses
  FROM org_num_addresses a
  LEFT JOIN clouddb.org_sfdc_accounts o ON a.org_id = o.org_id
  LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
  GROUP BY
    a.date,
    a.org_id,
    s.sam_number
),

org_num_alerts AS (
  SELECT
    c.date,
    g.organization_id as org_id,
    COUNT(DISTINCT a.id) as num_alerts
  FROM clouddb.alerts a
  LEFT JOIN clouddb.groups g on a.group_id = g.id
  LEFT JOIN definitions.445_calendar c
  ON
    c.date >= to_date(a.created_at) AND
    c.date <= current_date()
  GROUP BY
    c.date,
    g.organization_id
),

sam_num_alerts AS (
  SELECT
    a.date,
    a.org_id,
    s.sam_number,
    SUM(num_alerts) AS num_alerts
  FROM org_num_alerts a
  LEFT JOIN clouddb.org_sfdc_accounts o ON a.org_id = o.org_id
  LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
  GROUP BY
    a.date,
    a.org_id,
    s.sam_number
),

org_num_roles AS (
  SELECT
    c.date,
    r.org_id,
    COUNT(DISTINCT r.uuid) AS num_roles
  FROM clouddb.custom_roles r
  LEFT JOIN definitions.445_calendar c
  ON
    c.date >= to_date(r.created_at) AND
    c.date <= current_date()
  GROUP BY
    c.date,
    r.org_id
),

sam_num_roles AS (
  SELECT
    r.date,
    r.org_id,
    s.sam_number,
    SUM(num_roles) AS num_roles
  FROM org_num_roles r
  LEFT JOIN clouddb.org_sfdc_accounts o ON r.org_id = o.org_id
  LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
  GROUP BY
    r.date,
    r.org_id,
    s.sam_number
),

org_num_tags AS (
  SELECT
    c.date,
    t.org_id,
    COUNT(DISTINCT t.id) AS num_tags
  FROM clouddb.tags t
  LEFT JOIN definitions.445_calendar c
  ON
    c.date >= to_date(t.created_at) AND
    c.date <= current_date()
  GROUP BY
    c.date,
    t.org_id
),

sam_num_tags AS (
  SELECT
    t.date,
    t.org_id,
    s.sam_number,
    SUM(num_tags) AS num_tags
  FROM org_num_tags t
  LEFT JOIN clouddb.org_sfdc_accounts o ON t.org_id = o.org_id
  LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
  GROUP BY
    t.date,
    t.org_id,
    s.sam_number
),

org_api_calls AS (
  SELECT
    date,
    OrgId AS org_id,
    count(1) AS num_api_calls
  FROM api_logs.api
  WHERE date < '2020-11-12'
  GROUP BY
    date,
    OrgId

  UNION ALL

  SELECT
    date,
    org_id,
    count(1) AS num_api_calls
  FROM datastreams.api_logs
  GROUP BY
    date,
    org_id
),

sam_api_calls AS (
  SELECT
    a.date,
    a.org_id,
    s.sam_number,
    SUM(a.num_api_calls) AS num_api_calls
  FROM org_api_calls a
  LEFT JOIN clouddb.org_sfdc_accounts o ON a.org_id = o.org_id
  LEFT JOIN clouddb.sfdc_accounts s ON o.sfdc_account_id = s.id
  GROUP BY
    a.date,
    a.org_id,
    s.sam_number
)

SELECT
    org_dates.date,
    org_dates.org_id,
    org_dates.sam_number,
    reports.num_scheduled_reports,
    routes.num_routes,
    addresses.num_addresses,
    alerts.num_alerts,
    rol.num_roles,
    t.num_tags,
    u.num_cloud_users,
    api.num_api_calls
FROM customer360.org_dates org_dates
LEFT JOIN dataprep.sam_scheduled_reports reports ON reports.sam_number = org_dates.sam_number AND reports.date = org_dates.date
LEFT JOIN  sam_num_routes routes ON routes.org_id = org_dates.org_id AND routes.date = org_dates.date
LEFT JOIN  sam_num_alerts alerts ON alerts.org_id = org_dates.org_id AND alerts.date = org_dates.date
LEFT JOIN  sam_num_addresses addresses ON addresses.org_id = org_dates.org_id AND addresses.date = org_dates.date
LEFT JOIN  sam_num_roles rol ON rol.org_id = org_dates.org_id AND rol.date = org_dates.date
LEFT JOIN  sam_num_tags t ON t.org_id = org_dates.org_id AND t.date = org_dates.date
LEFT JOIN  sam_num_users u ON u.org_id = org_dates.org_id AND u.date = org_dates.date
LEFT JOIN sam_api_calls api ON api.org_id = org_dates.org_id AND api.date = org_dates.date
WHERE org_dates.date IS NOT NULL
AND org_dates.org_id IS NOT NULL
AND org_dates.date >= ${start_date}
AND org_dates.date < ${end_date}
