SELECT DISTINCT population_type
FROM product_analytics.dim_populations
WHERE population_type != ''
  AND date = (SELECT max(date) FROM product_analytics.dim_populations)
ORDER BY population_type

