WITH locations AS (
  SELECT
    date,
    org_id,
    device_id,
    time,
    value
  FROM kinesisstats.location loc
  WHERE date >= ${start_date}
  AND date < ${end_date}
),

intervals AS (
  SELECT
    *
  FROM engine_state_report.engine_state_driver_intervals loc
  WHERE date >= ${start_date}
  AND date < ${end_date}
  -- OFF intervals have no use in the Idling report so ignore them
  AND engine_state != "OFF"
),

intervals_with_first_and_last_loc AS (
  SELECT
    ei.date,
    ei.org_id,
    1 as object_type,
    ei.object_id,
    ei.start_ms,
    ei.end_ms,
    CASE
      WHEN ei.engine_state = 'OFF' THEN 0
      WHEN ei.engine_state = 'ON' THEN 1
      WHEN ei.engine_state = 'IDLE' THEN 2
      ELSE -1
    END as engine_state,
    ei.productive_pto_state as pto_state,
    ei.fuel_consumed_ml,
    ei.gaseous_fuel_consumed_grams,
    ei.avg_air_temp_c,
    ei.driver_id,
    MIN_BY(loc.value, loc.time) as first_loc,
    -- get the last non-null location point for this device
    -- in case we need to handle edge cases where no location is recorded during this engine interval
    LAST(MAX_BY(loc.value, loc.time), true) OVER (
        PARTITION BY ei.org_id, object_id
        ORDER BY
          end_ms ROWS BETWEEN UNBOUNDED PRECEDING
          AND CURRENT ROW
      ) AS last_loc
  FROM intervals ei LEFT JOIN locations loc
  ON loc.date = ei.date
    AND loc.org_id = ei.org_id
    AND loc.device_id = ei.object_id
    AND loc.time >= ei.start_ms
    AND loc.time < ei.end_ms
  GROUP BY ei.date, ei.org_id, ei.object_id, ei.start_ms, ei.end_ms, ei.engine_state, ei.productive_pto_state, ei.fuel_consumed_ml, ei.gaseous_fuel_consumed_grams, ei.avg_air_temp_c, ei.driver_id
),

intervals_with_location AS (
  SELECT
    date,
    org_id,
    object_type,
    object_id,
    start_ms,
    end_ms,
    engine_state,
    pto_state,
    fuel_consumed_ml,
    gaseous_fuel_consumed_grams,
    avg_air_temp_c,
    driver_id,
    -- if we do not have a first_location, then this interval did not have an overlapping location point
    -- in this case, use last_location instead which is the most recent non-null location point that did not overlap with the engine interval
    COALESCE(first_loc, last_loc) as location
  FROM intervals_with_first_and_last_loc
)

SELECT
  date,
  org_id,
  object_type,
  object_id,
  start_ms,
  end_ms,
  engine_state,
  pto_state,
  fuel_consumed_ml,
  gaseous_fuel_consumed_grams,
  avg_air_temp_c,
  driver_id,
  location.latitude AS starting_latitude,
  location.longitude AS starting_longitude,
  location.revgeo_house_number AS starting_house_number,
  location.revgeo_neighborhood AS starting_neighborhood,
  location.revgeo_street AS starting_street,
  location.revgeo_city AS starting_city,
  location.revgeo_state AS starting_state,
  location.revgeo_country AS starting_country,
  location.revgeo_postcode AS starting_post_code
FROM intervals_with_location
