SELECT
  *,
  CONCAT(
    DATE_ADD(TO_DATE(NOW()), -7),
    ' - ',
    TO_DATE(NOW())
  ) AS week
FROM
  perf_infra.org_route_loads
WHERE
  date >= DATE_ADD(DATE(NOW()), -7)
  AND Date < DATE(NOW())
