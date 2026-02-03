SELECT
  CAST(fl.org_id AS BIGINT) AS org_id,
  DATE(created_at) AS date
FROM
  fuelcardsdb_shards.fleetcor_integration_configs fl
WHERE
  fl.org_id IS NOT NULL
  AND COALESCE(DATE(fl.deleted_at), DATE_ADD('{PARTITION_START}', 1)) >= '{PARTITION_START}'

UNION ALL

SELECT
  CAST(fl.org_id AS BIGINT) AS org_id,
  DATE(created_at) AS date
FROM
  fuelcardintegrationsdb.wex_integration_configs fl
WHERE
  fl.org_id IS NOT NULL

UNION ALL

SELECT DISTINCT
  CAST(org_id AS BIGINT) AS org_id,
  DATE(installed_at) AS date
FROM product_analytics.dim_app_installs
WHERE
  date = '{PARTITION_START}'
  AND app_name IN (
      'Coast',
      'Coast Pay',
      'Allstar',
      'Fleevo Fleet Card Connector',
      'Multi Service Fuel Card',
      'Relay Fuel',
      'Voyager'
  )
