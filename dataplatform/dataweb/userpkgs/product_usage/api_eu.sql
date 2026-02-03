SELECT
  org_id,
  date,
  SUM(num_requests) AS num_requests
FROM data_tools_delta_share.product_analytics.fct_api_usage
WHERE
  status_code = 200
GROUP BY 1, 2
