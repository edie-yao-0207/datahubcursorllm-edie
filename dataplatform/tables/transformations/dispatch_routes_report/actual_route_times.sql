-- for each route_id, get the actual_start_ms and actual_end_ms
WITH
-- filter selected routes and jobs by the Spark job time window
  filtered_routes_and_jobs AS (
    SELECT *
    FROM dispatch_routes_report.selected_routes_and_jobs
    WHERE route_scheduled_or_updated_date >= ${start_date}
    AND route_scheduled_or_updated_date < ${end_date}
  ),

-- get last actual job
  last_actual_jobs AS (
    SELECT
      job_id,
      route_id,
      org_id,
      object_type,
      object_id,
      job_state,
      complete_last_stop_on_departure,
      arrived_at,
      completed_at,
      scheduled_start_ms,
      route_scheduled_or_updated_date
    FROM
      (
        SELECT
          job_id,
          route_id,
          org_id,
          object_type,
          object_id,
          job_state,
          complete_last_stop_on_departure,
          arrived_at,
          completed_at,
          scheduled_start_ms,
          route_scheduled_or_updated_date,
          row_number() OVER(PARTITION BY route_id ORDER BY arrived_at DESC, completed_at DESC) AS rn
        FROM filtered_routes_and_jobs
        -- 3 represents a completed job
        -- full description of job states can be found at
        -- https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/fleet/dispatch/dispatchproto/dispatch_routes.pb.go#L28-L35
        WHERE job_state = 3
      )
    WHERE rn = 1
  ),

-- get last scheduled job
  last_scheduled_jobs AS (
    SELECT
      job_id AS max_job_id,
      route_id
    FROM
      (
        SELECT
          job_id,
          route_id,
          scheduled_arrival_time,
          row_number() OVER(PARTITION BY route_id ORDER BY scheduled_arrival_time DESC, job_id DESC) AS rn
        FROM filtered_routes_and_jobs
      )
    WHERE rn = 1
  ),

-- get start time
  actual_start_times AS (
    SELECT
      route_id,
      -- min_time, as described in the subquery, is in seconds (due to unix_timestamp)
      -- to keep with the convention of using ms elsewhere in the report,
      -- we will multiply min_time by 1000 to convert seconds to ms
      MIN(min_time) * 1000 AS actual_start_ms
    FROM
      (
        SELECT
          route_id,
        CASE WHEN UNIX_TIMESTAMP(en_route_at) < UNIX_TIMESTAMP(completed_at) AND UNIX_TIMESTAMP(en_route_at) < UNIX_TIMESTAMP(arrived_at) AND UNIX_TIMESTAMP(en_route_at) > 0 THEN UNIX_TIMESTAMP(en_route_at)
             WHEN UNIX_TIMESTAMP(completed_at) < UNIX_TIMESTAMP(en_route_at) AND UNIX_TIMESTAMP(completed_at) < UNIX_TIMESTAMP(arrived_at) AND UNIX_TIMESTAMP(completed_at) > 0 THEN UNIX_TIMESTAMP(completed_at)
             ELSE NULLIF(UNIX_TIMESTAMP(arrived_at), 0)
             END AS min_time
        FROM filtered_routes_and_jobs
      )
    GROUP BY route_id
    HAVING actual_start_ms IS NOT NULL
  ),

-- get end time
  actual_end_times AS (
    SELECT
      last_actual_jobs.route_id,
      -- in the case where the last scheduled job is the same as the last actual job,
      -- and if the last stop was completed
      -- then we will use the arrived_at time as the end time
      -- otherwise, we will use the completed_at time as the end time
      IF (job_id=max_job_id AND complete_last_stop_on_departure = 1 AND job_state = 3, UNIX_TIMESTAMP(arrived_at) * 1000, UNIX_TIMESTAMP(completed_at) * 1000) AS actual_end_ms,
      scheduled_start_ms,
      route_scheduled_or_updated_date,
      org_id,
      object_type,
      object_id
    FROM last_actual_jobs
    JOIN last_scheduled_jobs
    ON last_actual_jobs.route_id=last_scheduled_jobs.route_id
  )

-- getting start & end ms for all routes
SELECT
  s.route_id,
  org_id,
  object_type,
  object_id,
  actual_start_ms,
  actual_end_ms,
  scheduled_start_ms,
  route_scheduled_or_updated_date
FROM actual_start_times AS s
JOIN actual_end_times AS e
ON s.route_id=e.route_id
WHERE actual_start_ms < actual_end_ms
