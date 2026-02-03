WITH engine_state_intervals AS (
  -- Map engine state transitions to intervals.
  SELECT
    date,
    org_id,
    object_id,
    time AS start_ms,
    CASE
      WHEN int_value = 0 THEN "OFF"
      WHEN int_value = 1 THEN "ON"
      WHEN int_value = 2 THEN "IDLE"
    END AS state,
    LEAD(time) OVER (PARTITION BY org_id, object_id ORDER BY time ASC) AS end_ms
  FROM engine_state.transitions
  WHERE date >= DATE_SUB(${start_date}, 3)
  AND date < DATE_ADD(${end_date}, 30)
),

-- we split engine intervals at day boundaries so when downstream nodes query for data by
-- ${start_date} they do not miss out on intervals where the node ends on some future date.
engine_intervals_by_covering_day AS (
  SELECT
    DATE(day_start) AS date,
    org_id,
    object_id,
    state,
    -- start the interval on the day's start if it starts on a previous day and end the interval
    -- at the day's end if it continues into the next day.
    GREATEST(start_ms, CAST(unix_timestamp(day_start) * 1000 AS BIGINT)) AS start_ms,
    LEAST(end_ms, CAST(unix_timestamp(day_start + INTERVAL 1 DAY) * 1000 AS BIGINT)) AS end_ms,
    end_ms - start_ms AS original_interval_duration_ms -- Keep track of the original interval duration for filtering.
  FROM(
    -- create new row for each day the interval spans.
    SELECT
      *,
      EXPLODE(
        SEQUENCE(
          DATE_TRUNC('day', from_unixtime(start_ms / 1000)),
          DATE_TRUNC('day', from_unixtime((end_ms - 1) / 1000)), -- SEQUENCE() takes inclusive end time.
          INTERVAL 1 day
        )
      ) AS day_start
    FROM engine_state_intervals
    -- Don't create an engine state interval between the last engine state
    -- transition and end_date or any other time. The data point could later
    -- be flagged as invalid due to a data break, in which case we may have
    -- counted a long time as engine on when it was actually off. If the engine
    -- state is valid, the next interval will be created in a later run.
    WHERE end_ms IS NOT NULL
  )
)

SELECT
  *
FROM engine_intervals_by_covering_day
WHERE date >= ${start_date}
AND date < ${end_date}

