
WITH segments AS (
  SELECT
      org_id,
      device_id,
      date,
      -- take the ms time and divide it by 600,000 (the number of ms in 10 minutes), then find the floor to group times within 10 minute intervals
      -- then multiply back out to an ms epoch timestamp so we can read the number
      FLOOR(time / (1000 * 60 * 10)) * 10 * 60 * 1000 as segment_start,
      -- a spotty ECU point is a point where the GPS speed is > 15 mph but there is no ECU speed reported
      SUM(CASE WHEN value.gps_speed_meters_per_second * 2.23694 > 15 AND value.has_ecu_speed IS NULL THEN 1
        ELSE 0
      END) / COUNT(*) AS pct_bad_ecu
    FROM
      kinesisstats.location
    WHERE
      date >= ${start_date} AND date < ${end_date}
    GROUP BY 1, 2, 3, 4
),

-- find days where some ECU was reported for a given vehicle
ecu_in_a_day AS (
  SELECT
    org_id,
    device_id,
    date,
    SOME(value.has_ecu_speed) AS has_some_ecu_speed_during_day
  FROM kinesisstats.location
  WHERE date >= ${start_date} AND date < ${end_date}
  GROUP BY org_id, device_id, date
  HAVING has_some_ecu_speed_during_day = true
)

-- join to filter down segments to only segments that occurred on days with some ECU reported
SELECT
  seg.org_id,
  seg.device_id,
  DATE(seg.date),
  seg.segment_start,
  seg.segment_start + 600000 AS segment_end,
  seg.pct_bad_ecu
FROM segments seg JOIN ecu_in_a_day ecu
ON seg.org_id = ecu.org_id AND seg.device_id = ecu.device_id AND seg.date = ecu.date
-- only use spotty ECU segments to filter if >= 75% of their points are spotty ECU
AND pct_bad_ecu >= 0.75
