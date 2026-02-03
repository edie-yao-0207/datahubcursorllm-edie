SELECT DISTINCT
  CAST(dai.org_id AS BIGINT) AS org_id,
  DATE(dai.installed_at) AS date,
  dai.app_uuid
FROM data_tools_delta_share.product_analytics.dim_app_installs dai
JOIN delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-clouddb/prod_db/developer_apps_v0` da
  ON dai.app_uuid = da.uuid
WHERE
  dai.date = '{PARTITION_START}'
  AND da.approval_state = 'Published'
  AND da.deactivated = 0
