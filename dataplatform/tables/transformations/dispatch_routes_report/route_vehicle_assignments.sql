WITH
-- get all the driver assignments
driver_assignments AS (
  SELECT
    route_id,
    actual_start_ms as route_actual_start_ms,
    actual_end_ms as route_actual_end_ms,
    scheduled_start_ms,
    route_scheduled_or_updated_date,
    object_id,
    org_id
  FROM dispatch_routes_report.actual_route_times
  WHERE route_scheduled_or_updated_date >= ${start_date}
  AND route_scheduled_or_updated_date < ${end_date}
  -- object_type of 0 corresponds to driver
  -- refer to stage 1: dataplatform/tables/transformations/dispatch_routes_report:selected_routes_and_jobs.sql
  AND object_type = 0
  -- filter out routes with actual duration longer than 30 days
  AND actual_end_ms - actual_start_ms <= 2592000000
),
-- get the already-vehicle assignmnts (dont need to be translated)
vehicle_assignments AS (
  SELECT
    route_id,
    actual_start_ms,
    actual_end_ms,
    scheduled_start_ms,
    route_scheduled_or_updated_date,
    object_id,
    org_id
  FROM dispatch_routes_report.actual_route_times
  WHERE route_scheduled_or_updated_date >= ${start_date}
  AND route_scheduled_or_updated_date < ${end_date}
  -- object_type of 1 corresponds to vehicle
  -- refer to stage 1: dataplatform/tables/transformations/dispatch_routes_report:selected_routes_and_jobs.sql
  AND object_type = 1
  -- filter out routes with actual duration longer than 30 days
  AND actual_end_ms - actual_start_ms <= 2592000000
),
-- get the min route actual start ms and max route actual end ms
-- for bounding hos assignment data
routes_actual_time_bounds AS (
  SELECT
    CAST(from_unixtime(MIN(route_actual_start_ms) / 1000) AS timestamp) AS min_route_actual_start,
    CAST(from_unixtime(MAX(route_actual_end_ms) / 1000) AS timestamp) AS max_route_actual_end
  FROM driver_assignments
),
-- pre-filter out any invalid hos assignments as well as those outside
-- the scope of the driver assignment actual times we are processing
filtered_hos_assignments AS (
  SELECT
    id AS driver_hos_id,
    date,
    driver_id,
    vehicle_id,
    org_id,
    UNIX_TIMESTAMP(start_at) * 1000 as assignment_start_ms,
    UNIX_TIMESTAMP(end_at) * 1000 as assignment_end_ms,
    is_passenger,
    session_uuid
  FROM compliancedb_shards.driver_hos_vehicle_assignments
  WHERE start_at >= (SELECT min_route_actual_start FROM routes_actual_time_bounds)
  AND (end_at IS NULL OR end_at <= (SELECT max_route_actual_end FROM routes_actual_time_bounds))
  AND (end_at IS NULL OR start_at <= end_at)
),
-- there are some duplicate rows in the driver_hos_vehicle_assignments table
-- deduplicate them here so that we don't get duplicate route vehicle assignment rows later
deduplicated_hos_assignments AS (
  SELECT
    driver_hos_id, date, driver_id, vehicle_id, org_id, assignment_start_ms, assignment_end_ms, is_passenger, session_uuid
  FROM filtered_hos_assignments
  GROUP BY driver_hos_id, date, driver_id, vehicle_id, org_id, assignment_start_ms, assignment_end_ms, is_passenger, session_uuid
),
-- match driver assignments to any overlapping vehicle assignments
driver_vehicle_mapping AS (
  SELECT
    date,
    driver_id,
    vehicle_id,
    driver_assignments.org_id,
    route_id,
    assignment_start_ms,
    assignment_end_ms,
    driver_hos_id,
    is_passenger,
    session_uuid,
    route_actual_start_ms,
    route_actual_end_ms,
    scheduled_start_ms,
    route_scheduled_or_updated_date
  FROM driver_assignments
  JOIN deduplicated_hos_assignments
  ON object_id = driver_id
  AND driver_assignments.org_id = deduplicated_hos_assignments.org_id
  AND (
        (assignment_end_ms > route_actual_start_ms AND assignment_start_ms < route_actual_end_ms)
        OR (assignment_end_ms IS NULL AND assignment_start_ms < route_actual_end_ms)
      )
),
-- find any route vehicle assignments which overlap with each other
-- overlaps will need to be processed out such that we end with a single vehicle assigned to a route at any given time
overlapping_routes AS (
  SELECT
    t1.*
  FROM driver_vehicle_mapping t1
  JOIN driver_vehicle_mapping t2
  ON t1.org_id = t2.org_id
  AND t1.driver_hos_id != t2.driver_hos_id
  AND t1.route_id = t2.route_id
  AND t1.driver_id = t2.driver_id
  AND (
    (t1.assignment_end_ms > t2.assignment_start_ms AND t1.assignment_start_ms < t2.assignment_end_ms)
    OR (t1.assignment_end_ms IS NULL AND t1.assignment_start_ms < t2.assignment_end_ms)
    OR (t2.assignment_end_ms IS NULL AND t2.assignment_start_ms < t1.assignment_end_ms)
    OR (t1.assignment_end_ms IS NULL AND t2.assignment_end_ms IS NULL)
  )
),
-- set aside the routes without overlaps
no_change_assignments AS (
  SELECT
    route_id,
    vehicle_id,
    org_id,
    IF (assignment_start_ms < route_actual_start_ms, route_actual_start_ms, assignment_start_ms) AS assignment_start_ms,
    IF (assignment_end_ms IS NULL OR assignment_end_ms > route_actual_end_ms, route_actual_end_ms, assignment_end_ms) AS assignment_end_ms,
    scheduled_start_ms,
    route_scheduled_or_updated_date
  FROM
    (SELECT * FROM driver_vehicle_mapping
    EXCEPT
    SELECT * FROM overlapping_routes)
),
-- create disjoint time ranges for each route and driver
constructed_ranges AS (
  SELECT
    *
  FROM
    (SELECT
      route_id,
      org_id,
      driver_id,
      vehicle_time_ms,
      LEAD(vehicle_time_ms) OVER (PARTITION BY route_id, driver_id ORDER BY vehicle_time_ms) AS next_vehicle_time_ms
    FROM
      (SELECT
        *
      FROM
        (SELECT
          route_id,
          org_id,
          driver_id,
          IF (assignment_start_ms < route_actual_start_ms, route_actual_start_ms, assignment_start_ms) AS vehicle_time_ms
        FROM overlapping_routes
        UNION
        SELECT
          route_id,
          org_id,
          driver_id,
          -- using this large time as the endpoint for an open time range
          IF (COALESCE(assignment_end_ms, 4294967296000) > route_actual_end_ms, route_actual_end_ms, COALESCE(assignment_end_ms, 4294967296000)) AS vehicle_time_ms
        FROM overlapping_routes
        )
      ORDER BY route_id, vehicle_time_ms ASC
      )
    )
  WHERE next_vehicle_time_ms IS NOT NULL
),
-- filter out overlapping constructed time ranges by prioritizing driver over passenger and breaking ties with larger driver_hos_id
no_overlapping_va AS (
  SELECT
    route_id,
    vehicle_id,
    driver_id,
    vehicle_time_ms,
    next_vehicle_time_ms,
    org_id,
    driver_hos_id,
    is_passenger,
    session_uuid,
    route_actual_start_ms,
    route_actual_end_ms,
    scheduled_start_ms,
    route_scheduled_or_updated_date
  FROM
    (SELECT
        va.route_id,
        va.vehicle_id,
        c.driver_id,
        c.vehicle_time_ms,
        c.next_vehicle_time_ms,
        c.org_id,
        va.driver_hos_id,
        va.is_passenger,
        va.session_uuid,
        va.route_actual_start_ms,
        va.route_actual_end_ms,
        va.scheduled_start_ms,
        va.route_scheduled_or_updated_date,
  -- want to create a partition on this unique combination for a unique non-overlapping time slot
  -- pick a row based on firstly if it's a driver
  -- secondly if the driver hos id is larger (more recently made)
        row_number() OVER(PARTITION BY va.route_id, va.driver_id, c.vehicle_time_ms, c.next_vehicle_time_ms ORDER BY va.is_passenger ASC, va.driver_hos_id DESC) AS rn
      FROM constructed_ranges AS c
      JOIN overlapping_routes AS va
      ON va.org_id = c.org_id
      AND va.route_id = c.route_id
      AND va.driver_id = c.driver_id
  -- logic from samsaradev.io/helpers/timerange/timerange.go:ContainsMsWithNowMs()
      AND ((va.assignment_start_ms == c.vehicle_time_ms AND va.assignment_end_ms == c.next_vehicle_time_ms)
          OR (va.assignment_start_ms <= c.vehicle_time_ms AND (va.assignment_end_ms IS NULL OR c.next_vehicle_time_ms <= va.assignment_end_ms))
        )
    )
  WHERE rn = 1
),
-- next 3 views are for consolidating consecutive time ranges for the same driver route to vehicle mappings
-- existing report logic can be found in go/src/samsaradev.io/fleet/compliance/compliancemodels/driver_hos_vehicle_assignment.go::filterDriverHosVehicleAssignments()
with_previous_va_properties AS (
  SELECT
    route_id,
    vehicle_id,
    driver_id,
    vehicle_time_ms,
    next_vehicle_time_ms,
    org_id,
    driver_hos_id,
    LAG(driver_hos_id) OVER (PARTITION BY route_id, driver_id ORDER BY vehicle_time_ms) AS prev_driver_hos_id,
    is_passenger,
    LAG(is_passenger) OVER (PARTITION BY route_id, driver_id ORDER BY vehicle_time_ms) AS prev_is_passenger,
    session_uuid,
    LAG(session_uuid) OVER (PARTITION BY route_id, driver_id ORDER BY vehicle_time_ms) AS prev_session_uuid,
    route_actual_start_ms,
    route_actual_end_ms,
    scheduled_start_ms,
    route_scheduled_or_updated_date
  FROM no_overlapping_va
),
-- each group of consecutive rows with identical properties per route and driver is assigned a rank
with_consecutive_va_properties_ranks AS (
  SELECT
    route_id,
    vehicle_id,
    vehicle_time_ms,
    next_vehicle_time_ms,
    org_id,
    route_actual_start_ms,
    route_actual_end_ms,
    scheduled_start_ms,
    route_scheduled_or_updated_date,
    SUM(CASE WHEN driver_hos_id = prev_driver_hos_id AND is_passenger = prev_is_passenger AND session_uuid = prev_session_uuid THEN 0 ELSE 1 END) OVER (PARTITION BY route_id, driver_id ORDER BY vehicle_time_ms) AS rank
  FROM with_previous_va_properties
),
consolidated_va AS (
  SELECT
    route_id,
    vehicle_id,
    org_id,
    route_actual_start_ms,
    route_actual_end_ms,
    scheduled_start_ms,
    route_scheduled_or_updated_date,
    MIN(vehicle_time_ms) AS assignment_start_ms,
    MAX(next_vehicle_time_ms) AS assignment_end_ms
  FROM with_consecutive_va_properties_ranks
  GROUP BY org_id, route_id, vehicle_id, route_actual_start_ms, route_actual_end_ms, scheduled_start_ms, route_scheduled_or_updated_date, rank
),
-- clamp assignment start and end times using the route actual start and end ms
time_bounded_va AS (
  SELECT
    route_id,
    vehicle_id,
    org_id,
    scheduled_start_ms,
    route_scheduled_or_updated_date,
    -- clamping logic found in reports/reportdefinitions/dispatchroute/createreport.go:getDispatchRoutesVehicleAssignments()
    IF (assignment_start_ms < route_actual_start_ms, route_actual_start_ms, assignment_start_ms) AS assignment_start_ms,
    IF (assignment_end_ms > route_actual_end_ms, route_actual_end_ms, assignment_end_ms) AS assignment_end_ms
  FROM consolidated_va
  WHERE assignment_start_ms < route_actual_end_ms AND assignment_end_ms > route_actual_start_ms
)
-- all route vehicle assignments
(
  -- driver routes with overlaps (require filtering)
  SELECT
    route_id,
    org_id,
    vehicle_id,
    scheduled_start_ms,
    route_scheduled_or_updated_date,
    assignment_start_ms,
    assignment_end_ms
  FROM time_bounded_va
)
UNION
-- driver routes without overlaps (don't require filtering)
(
  SELECT
    route_id,
    org_id,
    vehicle_id,
    scheduled_start_ms,
    route_scheduled_or_updated_date,
    assignment_start_ms,
    assignment_end_ms
  FROM no_change_assignments
)
UNION
-- vehicle routes (don't require transformation)
(
  SELECT
    route_id,
    org_id,
    object_id as vehicle_id,
    scheduled_start_ms,
    route_scheduled_or_updated_date,
    actual_start_ms as assignment_start_ms,
    actual_end_ms as assignment_end_ms
FROM vehicle_assignments
)
