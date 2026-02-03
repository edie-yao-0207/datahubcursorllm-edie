-- filter down to just this past week to decrease the amount of work done on this unpartitioned table
WITH last_week_combined_route_loads AS (
  SELECT *
  FROM perf_infra.elite_org_combined_route_loads
  WHERE
    date > date_add(NOW(), -7)
    AND raw_url IS NOT NULL
    -- filter down to only include big testing org
    -- this list should be in sync with https://github.com/samsara-dev/backend/blob/10e4cb88455f5111c23d7df210674744deadeb8a/dataplatform/tables/transformations/perf_infra/elite_org_metric_big_org_rps.sql#L78
    AND org_id = 2000012
)
-- find the most recent example of each route that is within 50ms of the p50, p90, p95, and p99
SELECT
	resource,
	name,
  FIRST(p50_url) p50_url,
  FIRST(p90_url) p90_url,
  FIRST(p95_url) p95_url,
  FIRST(p99_url) p99_url
FROM (
	SELECT
	  resource,
	  name,
	  FIRST(p50_url) OVER (
	    PARTITION BY resource,
	    name
	    ORDER BY
	      CASE
	        WHEN p50_url IS NULL THEN 2
	        ELSE 1
	      END,
	      timestamp DESC
	  ) p50_url,
	  FIRST(p90_url) OVER (
	    PARTITION BY resource,
	    name
	    ORDER BY
	      CASE
	        WHEN p90_url IS NULL THEN 2
	        ELSE 1
	      END,
	      timestamp DESC
	  ) p90_url,
	  FIRST(p95_url) OVER (
	    PARTITION BY resource,
	    name
	    ORDER BY
	      CASE
	        WHEN p95_url IS NULL THEN 2
	        ELSE 1
	      END,
	      timestamp DESC
	  ) p95_url,
	  FIRST(p99_url) OVER (
	    PARTITION BY resource,
	    name
	    ORDER BY
	      CASE
	        WHEN p99_url IS NULL THEN 2
	        ELSE 1
	      END,
	      timestamp DESC
	  ) p99_url
	FROM
	  (
	    SELECT
	      p_crl.resource,
	      p_crl.name,
	      crl.timestamp,
	      CASE
	        WHEN ABS(crl.duration_ms - p_crl.p50_duration) < 50 THEN CASE
	          WHEN CHARINDEX('?', crl.raw_url) > 0 THEN CONCAT(crl.raw_url, '&login_as_user_id=', crl.user_id)
	          ELSE CONCAT(crl.raw_url, '?login_as_user_id=', crl.user_id)
	        END
	        ELSE NULL
	      END AS p50_url,
	      CASE
	        WHEN ABS(crl.duration_ms - p_crl.p90_duration) < 50 THEN CASE
	          WHEN CHARINDEX('?', crl.raw_url) > 0 THEN CONCAT(crl.raw_url, '&login_as_user_id=', crl.user_id)
	          ELSE CONCAT(crl.raw_url, '?login_as_user_id=', crl.user_id)
	        END
	        ELSE NULL
	      END AS p90_url,
	      CASE
	        WHEN ABS(crl.duration_ms - p_crl.p95_duration) < 50 THEN CASE
	          WHEN CHARINDEX('?', crl.raw_url) > 0 THEN CONCAT(crl.raw_url, '&login_as_user_id=', crl.user_id)
	          ELSE CONCAT(crl.raw_url, '?login_as_user_id=', crl.user_id)
	        END
	        ELSE NULL
	      END AS p95_url,
	      CASE
	        WHEN ABS(crl.duration_ms - p_crl.p99_duration) < 50 THEN CASE
	          WHEN CHARINDEX('?', crl.raw_url) > 0 THEN CONCAT(crl.raw_url, '&login_as_user_id=', crl.user_id)
	          ELSE CONCAT(crl.raw_url, '?login_as_user_id=', crl.user_id)
	        END
	        ELSE NULL
	      END AS p99_url
	    FROM
	      last_week_combined_route_loads crl
	      INNER JOIN (
	        SELECT
	          resource,
	          name,
	          APPROX_PERCENTILE(duration_ms, 0.5) as p50_duration,
	          APPROX_PERCENTILE(duration_ms, 0.9) as p90_duration,
	          APPROX_PERCENTILE(duration_ms, 0.95) as p95_duration,
	          APPROX_PERCENTILE(duration_ms, 0.99) as p99_duration
	        FROM
	          last_week_combined_route_loads
	        GROUP BY
	          resource,
	          name
	      ) p_crl ON p_crl.resource = crl.resource
	      AND p_crl.name = crl.name
	      AND (
					-- differences of 50ms are basically identical, this fuzzier match allows us to prefer more-recent examples
	        ABS(crl.duration_ms - p_crl.p50_duration) < 50
	        OR ABS(crl.duration_ms - p_crl.p90_duration) < 50
	        OR ABS(crl.duration_ms - p_crl.p95_duration) < 50
	        OR ABS(crl.duration_ms - p_crl.p99_duration) < 50
	      )
			ORDER BY crl.timestamp DESC
	  )
)
-- group by route to remove duplicates
GROUP BY
  resource,
  name
