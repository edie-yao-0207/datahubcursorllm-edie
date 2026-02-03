SELECT DISTINCT build
FROM product_analytics.dim_populations
WHERE date = (SELECT max(date) FROM product_analytics.dim_populations)
  AND build IS NOT NULL
ORDER BY build DESC

