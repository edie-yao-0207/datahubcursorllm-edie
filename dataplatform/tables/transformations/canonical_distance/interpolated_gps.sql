-- The result of this node is a high granularity osdGpsDistance stat that uses
-- distances from kinesistats.location to interpolate between osdGpsDistance
-- points.

WITH gps_distance AS (
  SELECT
    org_id,
    date,
    object_id AS device_id,
    value.time as time_ms,
    STRUCT(
      value.time AS time_ms,
      value.double_value AS osd_gps
    ) AS current_gps
  FROM kinesisstats_window.osdgpsdistance
  WHERE DATE(date) >= DATE_SUB(${start_date}, 3)
  AND date < ${end_date}
  AND value.time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000
),

-- Join the osdgpsdistance (gps_distance) and location gpsdistance
-- (objectstat_diffs.location) tables together, keeping track of the prev/next
-- osdGpsDistance values. For osdGpsDistance rows, the prev/next osdGpsDistance
-- row is just itself.
all_gps AS (
  SELECT
    org_id,
    date,
    device_id,
    time_ms,
    gps.current_gps.osd_gps AS osd_gps,
    MAX(current_gps) OVER ascending_gps_window AS prev_gps,
    MIN(current_gps) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time_ms DESC, COALESCE(
        gps.current_gps,
        STRUCT(0 as time_ms, 0 as osd_gps)
      ) DESC
    ) AS next_gps,
    SUM(distance) OVER ascending_gps_window AS gps_sum
  FROM (
    SELECT
      org_id,
      date,
      device_id,
      time as time_ms,
      distance
    FROM objectstat_diffs.location
    WHERE date >= DATE_SUB(${start_date}, 3)
    AND date < ${end_date}
    AND time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000
  ) loc
  FULL JOIN gps_distance gps
    USING(org_id, date, device_id, time_ms)
  WINDOW ascending_gps_window AS (
    PARTITION BY org_id, device_id
    ORDER BY time_ms ASC, COALESCE(
      gps.current_gps,
      STRUCT(0 as time_ms, 0 as osd_gps)
    ) ASC
  )
),

-- Compute interpolated gps_distance. If we have an osd_gps value, use it,
-- otherwise calculate the delta to add to the last osd_gps point by taking the
-- current cumulative sum, subtracting the first sum in the window and
-- multiplying by a ratio between the total osdGpsDistance and total gps
-- location distances over the window.
gps_distances AS (
  SELECT
    org_id,
    date,
    device_id,
    time_ms,
    (CASE WHEN osd_gps IS NOT NULL THEN osd_gps ELSE (
      -- Only use location gps to interpolate if we have more than one in the
      -- window.
      CASE WHEN MAX(gps_sum) OVER gps_distance_window != MIN(gps_sum) OVER gps_distance_window THEN (
        COALESCE(
          (next_gps.osd_gps - prev_gps.osd_gps) / (
            MAX(gps_sum) OVER gps_distance_window - MIN(gps_sum) OVER gps_distance_window
          ), 0) * (
          gps_sum - MIN(gps_sum) OVER gps_distance_window
        ) + prev_gps.osd_gps
      ) ELSE prev_gps.osd_gps END
    ) END) AS gps_distance
  FROM all_gps
  WHERE prev_gps IS NOT NULL
  AND next_gps IS NOT NULL
  WINDOW gps_distance_window AS (
    PARTITION BY org_id, device_id, next_gps
  )
),

gps_distances_with_prev AS (
  SELECT
    org_id,
    date,
    device_id,
    time_ms,
    gps_distance,
    LAG(STRUCT(
      time_ms,
      gps_distance
    )) OVER gps_distance_window AS previous
  FROM gps_distances
  WINDOW gps_distance_window AS (
    PARTITION BY org_id, device_id
    ORDER BY time_ms ASC
  )
)

SELECT *
FROM gps_distances_with_prev
WHERE date >= ${start_date}
AND date < ${end_date}
AND previous IS NOT NULL
