SELECT DISTINCT product_program_id
FROM product_analytics.dim_populations
WHERE date = (SELECT max(date) FROM product_analytics.dim_populations)
  AND product_program_id IS NOT NULL
ORDER BY product_program_id

