--- This table is used to calculate the RPS score for the last 50 interactions of a route if last 7 days does not have atleast 50 interaction datapoint for a route.

--- Filter all the interactions that happened in the last 90 days for big test orgs and order these interactions by their occurence number. This will help us compute p95 for the last 50 interactions of a route.
WITH interaction_by_their_occurence_number AS (
    SELECT
        date,
        resource,
        type,
        name,
        duration_ms,
        ROW_NUMBER() OVER (
            PARTITION BY resource, name, type
            ORDER BY date DESC
        ) as rn
    FROM
        perf_infra.elite_org_combined_route_loads
    WHERE
        date < ${end_date}
        AND date > date_add(${start_date}, -88)
        AND lower(name) not like '%csv%'
        -- The list includes big orgs (Sysco, Clean Harbors, XPO, Estes)
        AND org_id in (45402, 31577, 74597, 20413)
),
-- Calculate the RPS score for the last 50 interactions of a route.
rps_last_50_interactions as (
SELECT
  date,
  resource,
  name,
  type,
  CASE WHEN type = 'route_load' THEN perf_infra.rlc(rolling_p95) ELSE perf_infra.uic(rolling_p95) END as score,
  -- score_times_count is helpful for aggregated scores, such as team or route
  -- rps
  CASE WHEN type = 'route_load' THEN perf_infra.rlc(rolling_p95) * count ELSE perf_infra.uic(rolling_p95) * count END as score_times_count,
  count,
  rolling_p95,
  most_recent_date
FROM
  (
    SELECT
      resource,
      date_add(DATE(${end_date}), -2) as date,
      name,
      type,
      APPROX_PERCENTILE(duration_ms, 0.95) / 1000 as rolling_p95,
      COUNT(*) as count,
      FIRST(date) as most_recent_date
    FROM
      interaction_by_their_occurence_number
    WHERE
      rn <= 50
    GROUP BY
      resource,
      name,
      type
  )
),
-- Pick the score from the last 7 days if there is more than 50 interaction datapoint for a route/user interaction in the last 7 days otherwise pick the score from the last 50 interactions with in the last 90 days.
combined_rps as (
  SELECT
  l.date,
  l.resource,
  l.name,
  l.type,
  most_recent_date,
  CASE WHEN l.count >= COALESCE(b.count, 0) THEN l.score ELSE b.score END as score,
  CASE WHEN l.count >= COALESCE(b.count, 0) THEN l.score_times_count ELSE b.score_times_count END as score_times_count,
  CASE WHEN l.count >= COALESCE(b.count, 0) THEN l.count ELSE b.count END as count,
  CASE WHEN l.count >= COALESCE(b.count, 0) THEN l.rolling_p95 ELSE b.rolling_p95 END as rolling_p95
FROM
  rps_last_50_interactions l
  LEFT JOIN perf_infra.elite_org_metric_big_org_rps b ON b.resource = l.resource AND b.name = l.name AND b.date = l.date AND b.type = l.type
)

-- Here, we join the big org rps with the elite org metric rps so that we can get interaction counts from
-- production. This will enable us to use production weights while computing RPS for big orgs.
SELECT
  b.*,
  m.count as count_prod,
  m.count*b.score as score_times_count_prod
FROM
combined_rps b
  JOIN perf_infra.elite_org_metric_rps m ON b.resource = m.resource
  AND b.date = m.date
  AND b.name = m.name
  AND b.type = m.type
