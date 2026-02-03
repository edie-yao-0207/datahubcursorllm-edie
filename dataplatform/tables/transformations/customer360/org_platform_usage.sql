WITH org_route_loads AS (
  SELECT
    date,
    org_id,
    COUNT(1) num_route_loads
  FROM datamodel_platform_silver.stg_cloud_routes
  GROUP BY
    date,
    org_id
),

org_driver_app_loads AS (
  SELECT
    date,
    OrgId AS org_id,
    COUNT(1) num_driver_app_loads
  FROM mobile_logs.mobile_app_log_events
  WHERE
    EventType LIKE '%GLOBAL_NAVIGATE%' AND
    date < '2020-11-02'
  GROUP BY
    date,
    org_id

  UNION ALL

  SELECT
    date,
    org_id,
    COUNT(1) num_driver_app_loads
  FROM datastreams.mobile_logs
  where event_type LIKE '%GLOBAL_NAVIGATE%'
  GROUP BY
    date,
    org_id
),

admin_app_loads AS (
  SELECT
    maan.mp_date AS date,
    scr.org_id,
    COUNT(1) num_admin_app_loads
  FROM mixpanel_samsara.troy_admin_global_navigate maan
  JOIN datamodel_platform_silver.stg_cloud_routes scr
    ON maan.distinct_id = scr.distinct_id
  GROUP BY
    maan.mp_date,
    org_id
),

org_num_users AS (
  SELECT
    date,
    org_id,
    COUNT(distinct distinct_id) num_users
  FROM datamodel_platform_silver.stg_cloud_routes
  GROUP BY
    date,
    org_id
)

SELECT
    d.date,
    d.org_id,
    d.sam_number,
    rl.num_route_loads,
    u.num_users,
    da.num_driver_app_loads,
    al.num_admin_app_loads
FROM customer360.org_dates d
LEFT JOIN org_route_loads rl ON d.date = rl.date AND d.org_id = rl.org_id
LEFT JOIN org_num_users u ON d.date = u.date AND d.org_id = u.org_id
LEFT JOIN org_driver_app_loads da ON d.date = da.date AND d.org_id = da.org_id
LEFT JOIN admin_app_loads al ON d.date = al.date AND d.org_id = al.org_id
WHERE d.org_id IS NOT NULL
AND d.date IS NOT NULL
AND d.date >= ${start_date}
AND d.date < ${end_date}
