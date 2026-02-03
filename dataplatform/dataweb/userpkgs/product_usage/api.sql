SELECT
  org_id,
  date,
  SUM(num_requests) AS num_requests
FROM product_analytics.fct_api_usage
WHERE
  status_code = 200
GROUP BY 1, 2
