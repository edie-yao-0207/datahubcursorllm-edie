SELECT DISTINCT
  CAST(dai.org_id AS BIGINT) AS org_id,
  DATE(dai.installed_at) AS date,
  dai.app_uuid
FROM product_analytics.dim_app_installs dai
JOIN clouddb.developer_apps da
  ON dai.app_uuid = da.uuid
WHERE
  dai.date = '{PARTITION_START}'
  AND da.approval_state = 'Published'
  AND da.deactivated = 0
