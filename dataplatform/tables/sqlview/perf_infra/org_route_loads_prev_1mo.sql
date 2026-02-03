SELECT
  *,
  CONCAT(
    DATE_ADD(TO_DATE(NOW()), -60),
    ' - ',
    DATE_ADD(TO_DATE(NOW()), -30)
  ) AS month
FROM
  perf_infra.org_route_loads
WHERE
  date >= DATE_ADD(DATE(NOW()), -60)
  AND Date < DATE_ADD(DATE(NOW()), -30)
