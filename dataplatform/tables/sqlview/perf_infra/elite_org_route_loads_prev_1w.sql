SELECT
  *,
  CONCAT(
    DATE_ADD(TO_DATE(NOW()), -14),
    ' - ',
    DATE_ADD(TO_DATE(NOW()), -7)
  ) AS week
FROM
  perf_infra.elite_org_route_loads
WHERE
  date >= DATE_ADD(DATE(NOW()), -14)
  AND Date < DATE_ADD(DATE(NOW()), -7)
