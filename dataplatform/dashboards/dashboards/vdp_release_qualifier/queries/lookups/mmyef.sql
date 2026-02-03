SELECT DISTINCT
  make,
  model,
  CAST(year AS STRING) AS year,
  engine_model,
  engine_type,
  fuel_type
FROM product_analytics.dim_populations
WHERE date = (SELECT max(date) FROM product_analytics.dim_populations)
ORDER BY make, model, year

