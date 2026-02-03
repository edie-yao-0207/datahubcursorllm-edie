-- Turn profile speed threshold values into intervals.
WITH speed_profile_threshold_intervals AS (
  SELECT
    TO_DATE(from_unixtime(created_at / 1000)) AS date,
    org_id,
    profile_id,
    meters_per_second,
    from_unixtime(created_at / 1000) AS interval_start,
    COALESCE(
      from_unixtime(
        (LEAD(created_at) OVER (PARTITION BY org_id, profile_id ORDER BY created_at ASC)) / 1000),
      CAST (${end_date} AS TIMESTAMP)
    ) AS interval_end
  FROM
    fueldb_shards.profile_speed_thresholds
),

-- Expand speed profile threshold intervals into hour intervals.
speed_profile_threshold_hour_intervals AS (
  SELECT
    DATE(interval_start) AS date, -- keep date in sync with the updated interval_start
    org_id,
    profile_id,
    interval_start,
    meters_per_second
  FROM (
    SELECT
      org_id,
      profile_id,
      EXPLODE(
        SEQUENCE(
          DATE_TRUNC('hour', interval_start),
          DATE_TRUNC('hour', interval_end), -- SEQUENCE() takes inclusive end time.
          INTERVAL 1 hour
        )
      ) AS interval_start,
      meters_per_second
    FROM
      speed_profile_threshold_intervals
    WHERE
      interval_start < interval_end AND interval_end >= CAST (${start_date} AS TIMESTAMP)
  )
)

-- Average custom speed profile threshold by hour, if there are multiple entries per hour.
SELECT
  date,
  org_id,
  profile_id,
  interval_start,
  interval_start + INTERVAL 1 HOUR AS interval_end,
  AVG(meters_per_second) AS custom_speed_profile_threshold_meters_per_second
FROM speed_profile_threshold_hour_intervals
WHERE date >= ${start_date} AND date < ${end_date}
GROUP BY org_id, profile_id, interval_start, date
