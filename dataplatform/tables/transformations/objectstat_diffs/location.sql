-- This node calculates distance diffs between subsequent gps location points.
-- This differs to GPS since it's more granular, but also includes points where
-- the device may not be moving.

WITH locations AS (
  SELECT
    org_id,
    date,
    device_id,
    value.time as time,
    -- Projects lat/long point onto epsg coordinate system to calculate distances: https://epsg.io/3857
    -- ST_Transform expects points in long/lat order, so flip coordinates first.
    ST_Transform(
      ST_FlipCoordinates(
        ST_Point(
          CAST(value.longitude AS DECIMAL(38, 10)),
          CAST(value.latitude AS DECIMAL(38, 10))
        )
      ), 'epsg:4326', 'epsg:3857'
    ) AS point,
    ST_Transform(
      ST_FlipCoordinates(
        ST_Point(
          CAST(previous.value.longitude AS DECIMAL(38, 10)),
          CAST(previous.value.latitude AS DECIMAL(38, 10))
        )
      ), 'epsg:4326', 'epsg:3857'
    ) AS previous
  FROM kinesisstats_window.location
  WHERE NOT(
    -- exclude values with missing lat/long, missing previous lat/long, or invalid lat/long
    value.longitude IS NULL
    OR value.latitude IS NULL
    OR previous IS NULL
    OR previous.value.longitude IS NULL
    OR previous.value.latitude IS NULL
    OR value.longitude <= -180 OR value.longitude >= 180
    OR value.latitude <= -85 OR value.latitude >= 85
    OR previous.value.longitude <= -180 OR previous.value.longitude >= 180
    OR previous.value.latitude <= -85 OR previous.value.latitude >= 85
  )
  AND DATE(date) >= ${start_date}
  AND DATE(date) < ${end_date}
)

SELECT
  org_id,
  date,
  device_id,
  time,
  ST_Distance(point, previous) AS distance
FROM locations
