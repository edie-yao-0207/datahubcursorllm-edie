SELECT
  *,
  CONCAT(
    DATE_ADD(TO_DATE(NOW()), -30),
    ' - ',
    TO_DATE(NOW())
  ) AS month
FROM
  perf_infra.elite_org_route_loads
WHERE
  date >= DATE_ADD(DATE(NOW()), -30)
  AND Date < DATE(NOW())
