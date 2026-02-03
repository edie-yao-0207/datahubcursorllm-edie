-- Get all the completed route ids from the jobs table. A route is considered complete if all its
-- jobs have skipped or completed state (and not all jobs are skipped state),
-- which means all job states are between 3 (completed) and 4 (skipped) but has at least one completed stop.
WITH completed_route_ids AS (
  SELECT DISTINCT
    dispatch_route_id AS route_id
  FROM
    dispatchdb_shards.shardable_dispatch_jobs
  GROUP BY
    dispatch_route_id
  HAVING
    MIN(job_state) = 3
      AND
    MAX(job_state) <= 4
),

-- Get the completed routes by filtering based on completed_route_ids.
completed_routes AS (
  SELECT
    id AS route_id,
    IF (driver_id IS NOT NULL, 0, 1) AS object_type,
    COALESCE(driver_id, vehicle_id) AS object_id,
    scheduled_start_ms,
    TO_DATE(from_unixtime(scheduled_start_ms / 1000)) AS route_scheduled_or_updated_date,
    scheduled_end_ms,
    start_location_lat,
    start_location_lng,
    start_location_address_id,
    scheduled_meters,
    complete_last_stop_on_departure
  FROM
    dispatchdb_shards.shardable_dispatch_routes sdr
  JOIN
    completed_route_ids
  ON
    completed_route_ids.route_id = sdr.id
  WHERE
    ((vehicle_id IS NOT NULL AND driver_id IS NULL) OR
     (vehicle_id IS NULL AND driver_id IS NOT NULL))
    AND scheduled_start_ms < scheduled_end_ms
),

-- Completed dispatch routes with scheduled start within the current Spark job time window
filtered_completed_routes AS (
  SELECT
    *
  FROM
    completed_routes
  WHERE
    route_scheduled_or_updated_date >= ${start_date}
    AND route_scheduled_or_updated_date < ${end_date}
),

-- Route ids which have been updated within the current Spark job time window
recently_updated_route_ids AS (
  SELECT
    id as route_id,
    TO_DATE(updated_at) as route_scheduled_or_updated_date
  FROM
    dispatchdb_shards.shardable_dispatch_routes
  WHERE
    updated_at IS NOT NULL
    AND TO_DATE(updated_at) >= ${start_date}
    AND TO_DATE(updated_at) < ${end_date}
    AND id NOT IN (
      SELECT route_id FROM filtered_completed_routes
    )
),

-- Route ids which have have a dispatch job that was updated within the current Spark job time window
recently_updated_job_route_ids AS (
  SELECT
    dispatch_route_id as route_id,
    TO_DATE(MAX(updated_at)) as route_scheduled_or_updated_date
  FROM
    dispatchdb_shards.shardable_dispatch_jobs
  WHERE
    updated_at IS NOT NULL
    AND TO_DATE(updated_at) >= ${start_date}
    AND TO_DATE(updated_at) < ${end_date}
    AND dispatch_route_id NOT IN (
      SELECT route_id FROM filtered_completed_routes
    )
    AND dispatch_route_id NOT IN (
      SELECT route_id FROM recently_updated_route_ids
    )
  GROUP BY
    dispatch_route_id
),

all_recently_updated_routes_ids AS (
  SELECT *
  FROM recently_updated_route_ids
  UNION
  SELECT *
  FROM recently_updated_job_route_ids
),

-- Completed dispatch routes with a recently updated dispatch job
recently_updated_routes AS (
  SELECT
    completed_routes.route_id,
    object_type,
    object_id,
    scheduled_start_ms,
    all_recently_updated_routes_ids.route_scheduled_or_updated_date,
    scheduled_end_ms,
    start_location_lat,
    start_location_lng,
    start_location_address_id,
    scheduled_meters,
    complete_last_stop_on_departure
  FROM
    completed_routes
  JOIN
    all_recently_updated_routes_ids
  ON
    completed_routes.route_id = all_recently_updated_routes_ids.route_id
),

-- The dispatch routes we will return
-- These CTEs are guaranteed to be disjoint
selected_routes AS (
  SELECT *
  FROM filtered_completed_routes
  UNION
  SELECT *
  FROM recently_updated_routes
)

-- Get the dispatch jobs associated with the routes we'd like to return
SELECT
  sdj.id AS job_id,
  selected_routes.route_id AS route_id,
  sdj.org_id AS org_id,
  object_type,
  object_id,
  start_location_lat,
  start_location_lng,
  destination_lat,
  destination_lng,
  en_route_at,
  arrived_at,
  completed_at,
  skipped_at,
  scheduled_meters,
  scheduled_start_ms,
  route_scheduled_or_updated_date,
  scheduled_end_ms,
  scheduled_arrival_time,
  complete_last_stop_on_departure,
  job_state
FROM
  selected_routes
JOIN
  dispatchdb_shards.shardable_dispatch_jobs sdj
ON
  selected_routes.route_id = sdj.dispatch_route_id
