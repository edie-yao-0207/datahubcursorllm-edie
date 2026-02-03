-- helpful for filling in missing dates
WITH resource_dates AS (
  SELECT
    date_add(${start_date}, s.i - 7) AS date,
    rc.path AS resource
  FROM
    (
      SELECT
        posexplode(
          split(
            space(
              datediff(
                ${end_date},
                date_add(${start_date}, -7)
              )
            ),
            ' '
          )
        ) AS (i, x)
    ) s
    CROSS JOIN (
      SELECT
        DISTINCT(path) AS path
      FROM
        perf_infra.route_config
    ) rc
)
SELECT
  date,
  resource,
  name,
  type,
  CASE WHEN type = 'route_load' THEN perf_infra.rlc(FIRST(rolling_p95)) ELSE perf_infra.uic(FIRST(rolling_p95)) END as score,
  -- score_times_count is helpful for aggregated scores, such as team or route
  -- rps
  CASE WHEN type = 'route_load' THEN perf_infra.rlc(FIRST(rolling_p95)) * FIRST(count) ELSE perf_infra.uic(FIRST(rolling_p95)) * FIRST(count) END as score_times_count,
  FIRST(count) as count,
  FIRST(rolling_p95) as rolling_p95
FROM
  (
    SELECT
      rd.resource,
      rd.date,
      crl.name,
      crl.type,
      -- gets the p95 of the metric over the past week
      APPROX_PERCENTILE(crl.duration_ms, 0.95) OVER (
        PARTITION BY rd.resource,
        crl.name,
        crl.type
        ORDER BY
          CAST(rd.date AS timestamp) ASC RANGE BETWEEN INTERVAL 6 days PRECEDING
          AND CURRENT ROW
      ) / 1000 as rolling_p95,
      COUNT(*) OVER (
        PARTITION BY rd.resource,
        crl.name,
        crl.type
        ORDER BY
          CAST(rd.date AS timestamp) ASC RANGE BETWEEN INTERVAL 6 days PRECEDING
          AND CURRENT ROW
      ) as count
    FROM
      perf_infra.elite_org_combined_route_loads crl
      -- the purpose of joining resource_dates is to fill in missing dates which
      -- we may have no data for
      FULL OUTER JOIN resource_dates rd ON crl.date = rd.date
      AND crl.resource = rd.resource
    WHERE
      -- subtract 7 days from the start date, since we compute a 7-day rolling average
      rd.date >= date_add(${start_date}, -7)
      AND rd.date < ${end_date}
      -- filter out csvs as they should not be considered alongside other
      -- interactions
      AND lower(name) not like '%csv%'
  )
WHERE
  date >= ${start_date}
  AND date < ${end_date}
  -- don't consider metrics which won't have a valid p95
  AND count > 20
GROUP BY
  resource,
  date,
  name,
  type
