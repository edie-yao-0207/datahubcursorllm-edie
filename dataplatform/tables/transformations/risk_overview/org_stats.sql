
WITH
  all_orgs AS (
    SELECT id AS org_id
    FROM clouddb.organizations
    WHERE id IS NOT NULL
  ),
  driver_filtered_counts AS (
    SELECT org_id, COUNT(*) AS ct
    FROM risk_overview.driver_filtered
    GROUP BY org_id
  ),
  driver_filtered_app_counts AS (
    SELECT date, org_id, COUNT(*) AS ct
    FROM risk_overview.driver_filtered_app
    GROUP BY date, org_id
  ),
  camera_driver_facing_active_counts AS (
    SELECT date, org_id, COUNT(*) AS ct
    FROM risk_overview.camera_driver_facing_active
    GROUP BY date, org_id
  )

SELECT
  driver_filtered_app_counts.date,
  all_orgs.org_id,
  COALESCE(driver_filtered_counts.ct, 0) AS active_drivers,
  COALESCE(driver_filtered_app_counts.ct, 0) AS active_drivers_with_app,
  COALESCE(camera_driver_facing_active_counts.ct, 0) AS active_driver_facing_cameras
FROM all_orgs
  LEFT OUTER JOIN driver_filtered_counts ON all_orgs.org_id = driver_filtered_counts.org_id
  LEFT OUTER JOIN driver_filtered_app_counts ON all_orgs.org_id = driver_filtered_app_counts.org_id
  LEFT OUTER JOIN camera_driver_facing_active_counts
    ON all_orgs.org_id = camera_driver_facing_active_counts.org_id
    AND driver_filtered_app_counts.date = camera_driver_facing_active_counts.date
