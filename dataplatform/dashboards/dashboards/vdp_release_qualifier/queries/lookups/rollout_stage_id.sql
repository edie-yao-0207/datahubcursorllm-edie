SELECT DISTINCT CAST(rollout_stage_id AS STRING) AS rollout_stage_id
FROM product_analytics.dim_populations
WHERE date = (SELECT max(date) FROM product_analytics.dim_populations)
  AND rollout_stage_id IS NOT NULL
ORDER BY rollout_stage_id

