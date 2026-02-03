SELECT DISTINCT market
FROM product_analytics.dim_populations
WHERE date = (SELECT max(date) FROM product_analytics.dim_populations)
  AND market IS NOT NULL
ORDER BY market

