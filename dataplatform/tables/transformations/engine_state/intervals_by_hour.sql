-- Copied from 'feer.engine_state_intervals_by_hour_v2'.
-- Expand engine state intervals by covering hours.
WITH engine_state_intervals_by_covering_hour AS (
  SELECT
    date,
    org_id,
    object_id,
    state,
    start_ms,
    end_ms,
    EXPLODE(
      SEQUENCE(
        DATE_TRUNC('hour', from_unixtime(start_ms / 1000)),
        DATE_TRUNC('hour', from_unixtime(end_ms / 1000)), -- SEQUENCE() takes inclusive end time.
        INTERVAL 1 hour
      )
    ) AS hour_start
  FROM engine_state.intervals
  WHERE start_ms < end_ms
  AND date >= ${start_date}
  AND date < ${end_date}
),

-- Split engine state intervals by hour.
engine_state_intervals_by_hour AS (
  SELECT
    DATE(hour_start) AS date, -- keep date in sync with each hour interval start
    org_id,
    object_id,
    state,
    GREATEST(start_ms, CAST(unix_timestamp(hour_start) * 1000 AS BIGINT)) AS start_ms,
    LEAST(end_ms, CAST(unix_timestamp(hour_start + INTERVAL 1 HOUR) * 1000 AS BIGINT)) AS end_ms
  FROM engine_state_intervals_by_covering_hour
  WHERE date >= ${start_date}
  AND date < ${end_date}
)

-- Filter out intervals where start_ms and end_ms are equivalent because they cause duplicate
-- primary key rows and have no effect on the total
SELECT *
FROM engine_state_intervals_by_hour
WHERE start_ms != end_ms

