-- This node labels osdodometer rows as valid or invalid. Note: this won't
-- correctly catch invalid rows if there aren't any valid ones within 24 hours.

-- Label invalid events. These events include both jumps in total distance that
-- are 10 km and 200 m/s greater or less than the previous point.
WITH odo_with_invalid AS (
  SELECT
    org_id,
    date,
    object_id AS device_id,
    value.time AS time_ms,
    value.int_value AS osd_odometer,
    ABS(value.int_value - previous.value.int_value) > 50000 AS dist_over_50km,
    ABS(
      (value.int_value - previous.value.int_value) / (value.time - previous.value.time)
    ) > (200 / 1000) AS speed_over_200mps
  FROM kinesisstats_window.osdodometer
  WHERE DATE(date) >= DATE_SUB(${start_date}, 1)
  AND DATE(date) < ${end_date}
),

-- Group together rows that satisfy both distance and speed conditions with rows
-- following the invalid event by performing a cumulative sum.
odo_with_anomalies_seen AS (
  SELECT
    org_id,
    date,
    device_id,
    time_ms,
    osd_odometer,
    SUM(IF(dist_over_50km AND speed_over_200mps, 1, 0)) OVER (
      PARTITION BY org_id, device_id
      ORDER BY time_ms ASC
    ) AS anomalies_seen
  FROM odo_with_invalid
),

-- Based on anomalies, label rows to be filtered out. This is done by looking in
-- the range of points 1 day before/after a value and finding which side of an
-- anomaly event is most common. We will discard points that occur on the less
-- common side, removing any anomalies and points resulting from that anomaly.
-- Note: this isn't guaranteed to filter out anomalies with a duration longer
-- than a day.
odo_with_filtered AS (
  SELECT
    org_id,
    date,
    device_id,
    time_ms,
    osd_odometer,
    anomalies_seen,
    -- Finding the average of parity(anomalies_seen) will return the most common
    -- parity once rounded since we're taking the average of 1s and 0s.
    (
      (SUM(MOD(anomalies_seen, 2)) OVER points_in_window) /
      (COUNT(*) OVER points_in_window)
    ) AS majority_parity
  FROM odo_with_anomalies_seen
  WINDOW points_in_window AS (
    PARTITION BY org_id, device_id
    ORDER BY time_ms ASC
    RANGE BETWEEN 24 * 60 * 60 * 1000 PRECEDING
    AND 24 * 60 * 60 * 1000 FOLLOWING
  )
)

-- Filter out rows where the parity of anomalies_seen is not the majority
-- parity as well as helper rows before our time range. 
SELECT
  org_id,
  date,
  device_id,
  time_ms,
  osd_odometer,
  MOD(anomalies_seen, 2) = ROUND(majority_parity) as valid
FROM odo_with_filtered
WHERE date >= ${start_date}
AND date < ${end_date}
