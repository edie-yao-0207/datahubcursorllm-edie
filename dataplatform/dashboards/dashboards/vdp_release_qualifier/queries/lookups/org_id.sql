SELECT DISTINCT CAST(org_id AS STRING) AS org_id
FROM product_analytics.dim_populations
WHERE org_id IS NOT NULL
  AND date = (SELECT max(date) FROM product_analytics.dim_populations)
ORDER BY org_id

