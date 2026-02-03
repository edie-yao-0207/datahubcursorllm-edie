SELECT DISTINCT cable_name
FROM product_analytics.dim_populations
WHERE date = (SELECT MAX(date) FROM product_analytics.dim_populations)
  AND cable_name IS NOT NULL
ORDER BY cable_name

