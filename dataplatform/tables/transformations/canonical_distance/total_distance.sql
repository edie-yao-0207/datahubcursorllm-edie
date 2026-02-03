-- The result of this node is the final computed distance deltas.
WITH excluded_device_ids AS (
  SELECT
    id,
    org_id
  FROM productsdb.devices
  WHERE STRUCT(
    LOWER(make) AS make,
    LOWER(model) AS model
  ) IN (
    SELECT make, model
    FROM definitions.canonical_distance_excluded_mmys
  )
),

filtered_odo AS (
  SELECT
    org_id,
    date,
    device_id,
    time_ms,
    osd_odometer,
    LAG(STRUCT(
      time_ms,
      date,
      osd_odometer,
      1 AS valid
    )) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time_ms ASC
    ) AS previous
  FROM canonical_distance.filtered_odometer
  WHERE valid = true
  AND date >= DATE_SUB(${start_date}, 3)
  AND date < ${end_date}
  AND time_ms <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000
  AND STRUCT(
    device_id,
    org_id
  ) NOT IN (SELECT id, org_id FROM excluded_device_ids)
),

deduped_odo AS (
  SELECT
    org_id,
    date,
    device_id,
    time_ms,
    osd_odometer,
    STRUCT(
      time_ms,
      date,
      osd_odometer,
      1 AS valid
    ) AS current_odo,
    LAG(STRUCT(
      time_ms,
      date,
      osd_odometer,
      1 AS valid
    )) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time_ms ASC
    ) AS prev_odo
  FROM filtered_odo
  WHERE previous IS NULL
  OR previous.date < DATE_SUB(date, 3)
  OR osd_odometer > previous.osd_odometer
),

gps AS (
  SELECT *
  FROM canonical_distance.interpolated_gps
  WHERE date >= DATE_SUB(${start_date}, 3)
  AND date < ${end_date}
  AND time_ms <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000
),

-- Full join of odometer and interpolatedGpsDistance tables.
odo_with_gps AS (
  SELECT
    org_id,
    date,
    device_id,
    time_ms,
    current_odo,
    prev_odo,
    osd_odometer,
    gps_distance,
    previous AS prev_gps
  FROM deduped_odo odos
  FULL JOIN gps gps
  USING(org_id, device_id, date, time_ms)
),

-- Add info about the previous/next odometer values which will be used to
-- determine the individual distance deltas during the interpolation step.
-- Default prev_odo/next_odo to an invalid struct instead of null since grouping
-- by null doesn't work well.
odo_with_prev_next AS (
  SELECT
    org_id,
    date,
    device_id,
    time_ms,
    osd_odometer,
    gps_distance,
    prev_gps,
    COALESCE(prev_odo, MAX(current_odo) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time_ms ASC
    ), STRUCT(0 AS time_ms, '' AS date, 0 AS osd_odometer, 0 AS valid)) AS prev_odo,
    COALESCE(MAX(current_odo) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time_ms ASC
    ), STRUCT(0 AS time_ms, '' AS date, 0 AS osd_odometer, 0 AS valid)) AS current_odo,
    COALESCE(MIN(current_odo) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time_ms DESC
    ), STRUCT(0 AS time_ms, '' AS date, 0 AS osd_odometer, 0 AS valid)) AS next_odo
  FROM odo_with_gps
  WHERE
    date >= DATE_SUB(${start_date}, 3)
    AND date < ${end_date}
),

-- Table of total gps deltas between each pair of odometer values.
gps_deltas AS (
  SELECT
    org_id,
    device_id,
    prev_odo,
    next_odo,
    SUM(gps_distance - prev_gps.gps_distance) AS gps_delta
  FROM odo_with_prev_next
  GROUP BY org_id, device_id, prev_odo, next_odo
),

-- Label any gps deltas that need to be adjusted with odo interpolation using
-- our table of total gps deltas and the total odo delta between the next and
-- previous odo values.
-- Logic for whether we use the odo window for interpolation is:
--     1. Check if there is a next_odo and that both prev_odo and next_odo are valid
--     2. Check whether the difference is valid (same logic as filtering)
--     3. Check if the two points are < 3 days apart (because of our rolling computation window)
-- Otherwise, just use the gps delta.
gps_with_deltas AS (
  SELECT
    org_id,
    date,
    device_id,
    time_ms,
    osd_odometer,
    prev_odo,
    current_odo,
    next_odo,
    gps_distance,
    prev_gps,
    gps_delta,
    CASE WHEN (
      next_odo.valid = 1 AND prev_odo.valid = 1
      AND (
        ABS(next_odo.osd_odometer - prev_odo.osd_odometer) <= 10000
        OR (
          ABS((next_odo.osd_odometer - prev_odo.osd_odometer) / (next_odo.time_ms - prev_odo.time_ms)) <= (200 / 1000)
          AND ABS(next_odo.osd_odometer - prev_odo.osd_odometer) <= 100000
        )
      )
      AND prev_odo.date >= DATE_SUB(next_odo.date, 3)
    ) THEN 1 ELSE 0 END AS need_interpolation
  FROM odo_with_prev_next odo
    LEFT JOIN gps_deltas gps USING(org_id, device_id, prev_odo, next_odo)
),

-- Calculate distances for both ranges that need interpolation as well as those
-- with only odometer deltas using the following logic:
-- 1. If interpolation is not needed just use the gps diff else...
-- 2. Check if there's a gps delta for the range. If there isn't one, then use
--    the odometer delta.
-- 3. Check if the there's a prev_gps value. If not, then use 0, if so use the
--    interpolated value. Note: since this logic will be used in concurrent runs
--    for the ranges (current - 2, current + 1) and (current - 30, current - 3),
--    we need to distinguish between ranges including live data and backfills to
--    avoid including trailing, non-interpolated gps points at the end of the
--    monthly range.
distances AS (
  SELECT
    /*+ RANGE_JOIN(t1, 1) */
    t1.org_id,
    date,
    t1.device_id,
    time_ms,
    CASE
      WHEN need_interpolation = 1 THEN (
      CASE WHEN (gps_delta IS NULL OR gps_delta = 0) AND osd_odometer IS NOT NULL THEN (
        next_odo.osd_odometer - prev_odo.osd_odometer
      ) ELSE (
        CASE WHEN prev_gps IS NOT NULL THEN (
                CASE WHEN next_odo.osd_odometer > prev_odo.osd_odometer THEN (
                 -- if the odometer is increasing and the delta is positive, use the gps delta to calculate the distance
                    (gps_distance - prev_gps.gps_distance) * (next_odo.osd_odometer - prev_odo.osd_odometer) / gps_delta
                  )
                  ELSE gps_distance - prev_gps.gps_distance
                END
        ) ELSE 0 END
      ) END
    ) ELSE gps_distance - prev_gps.gps_distance END AS distance,
    need_interpolation AS interpolated,
    current_odo
  FROM gps_with_deltas t1
  WHERE NOT (
    (prev_odo.valid = 0 AND next_odo.valid = 1)
    OR (
      ${end_date} <= CURRENT_DATE()
      AND date > DATE_SUB(${end_date}, 3)
      AND prev_odo.valid = 1
      AND next_odo.valid = 0
    )
  )
)

SELECT
  org_id,
  date,
  device_id,
  time_ms,
  distance,
  interpolated,
  IF(current_odo.valid=1, current_odo.osd_odometer, NULL) AS osd_odometer
FROM distances
WHERE date >= ${start_date}
AND date < ${end_date}
