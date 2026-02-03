WITH
-- fetching driver vehicle mappings
route_vehicle_assignments AS (
  SELECT
    route_id,
    org_id,
    vehicle_id,
    scheduled_start_ms,
    route_scheduled_or_updated_date,
    assignment_start_ms,
    assignment_end_ms
  FROM dispatch_routes_report.route_vehicle_assignments
  WHERE route_scheduled_or_updated_date >= ${start_date}
  AND route_scheduled_or_updated_date < ${end_date}
  -- filter out routes with actual duration longer than 30 days
  -- to ensure implicitly bound location data
  AND assignment_end_ms - assignment_start_ms <= 2592000000
),
-- get the min route actual start ms and max route actual end ms
-- for bounding location data
routes_actual_time_bounds AS (
  SELECT
    TO_DATE(from_unixtime(MIN(assignment_start_ms) / 1000)) AS min_actual_start,
    TO_DATE(from_unixtime(MAX(assignment_end_ms) / 1000)) AS max_actual_end
  FROM route_vehicle_assignments
),

-- prefiltering locations table using route actual time bounds
location_stats_bounded AS (
  SELECT
    date,
    org_id,
    device_id,
    time,
    value.latitude AS latitude,
    value.longitude AS longitude
  FROM
    kinesisstats.location
  WHERE date >= (SELECT min_actual_start FROM routes_actual_time_bounds)
  AND date <= (SELECT max_actual_end FROM routes_actual_time_bounds)
  AND value.longitude IS NOT NULL
  AND value.latitude IS NOT NULL
  AND value.longitude >= -180
  AND value.longitude <= 180
  AND value.latitude >= -90
  AND value.latitude <= 90
),

-- getting all the coords associated with trips at specified times
vehicle_coordinates AS (
  SELECT
    route_id,
    va.org_id AS org_id,
    date,
    time,
    vehicle_id,
    latitude,
    longitude
  FROM route_vehicle_assignments va
  JOIN location_stats_bounded l
  ON va.org_id = l.org_id
  AND vehicle_id = device_id
  AND time >= assignment_start_ms
  AND time <= assignment_end_ms
),

-- calculating distance per route and vehicle using the haversine formula
haversine_calc AS (
  SELECT
    -- 6373 * 1000 represents the radius of the Earth in meters
    2 * atan2(sqrt(x), sqrt(IF(0 > 1 - x, 0, 1 - x))) * 6373 * 1000 AS dist,
    route_id,
    vehicle_id
  FROM
    (
      SELECT
        dlat * dlat + dlng * dlng * cos(rad_lat) * cos(rad_next_lat) AS x,
        route_id,
        vehicle_id
      FROM
        (
          SELECT
            sin(0.5 * (rad_next_lat - rad_lat)) AS dlat,
            sin(0.5 * (rad_next_lng - rad_lng)) AS dlng,
            rad_lat,
            rad_next_lat,
            route_id,
            vehicle_id
          FROM
            (
              SELECT
                radians(latitude) AS rad_lat,
                radians(next_lat) AS rad_next_lat,
                radians(longitude) AS rad_lng,
                radians(next_lng) AS rad_next_lng,
                route_id,
                vehicle_id
              FROM
                (
                  SELECT
                    latitude,
                    LEAD(latitude) OVER (PARTITION BY route_id, vehicle_id ORDER BY date, time) AS next_lat,
                    longitude,
                    LEAD(longitude) OVER (PARTITION BY route_id, vehicle_id ORDER BY date, time) AS next_lng,
                    route_id,
                    vehicle_id
                  FROM vehicle_coordinates
                )
              WHERE next_lat IS NOT NULL AND next_lng IS NOT NULL
            )
        )
    )
)

-- summing final distance for each route and vehicle
SELECT
  route_id,
  org_id,
  vehicle_id,
  actual_meters,
  scheduled_start_ms,
  route_scheduled_or_updated_date
FROM
  (
    SELECT
      SUM(COALESCE(dist, 0)) AS actual_meters,
      route_id AS r_id,
      vehicle_id AS v_id
    FROM haversine_calc
    GROUP BY route_id, vehicle_id
  )
JOIN route_vehicle_assignments
ON route_id = r_id
AND vehicle_id = v_id
