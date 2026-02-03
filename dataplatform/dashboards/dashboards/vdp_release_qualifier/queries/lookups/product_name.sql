SELECT DISTINCT product_name
FROM product_analytics.dim_populations
WHERE date = (SELECT MAX(date) FROM product_analytics.dim_populations)
  AND product_name IS NOT NULL
ORDER BY product_name

