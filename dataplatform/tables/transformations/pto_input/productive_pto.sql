WITH ports_last_active AS (
  SELECT
    org_id,
    device_id,
    port,
    MAX(time) AS port_last_active_ms
  FROM pto_input.transitions
  WHERE date >= DATE_SUB(${start_date}, 14)
    AND date < ${start_date}
    AND value = 1
  GROUP BY org_id, device_id, port
),

transitions_with_last_active AS (
  SELECT
    DATE(ptot.date) as date,
    ptot.org_id,
    ptot.device_id,
    ptot.port,
    ptot.time,
    ptot.value,
    ptot.is_productive
  FROM pto_input.transitions ptot
  LEFT JOIN ports_last_active pla
    ON ptot.org_id = pla.org_id
    AND ptot.device_id = pla.device_id
    AND ptot.port = pla.port
  -- Grab transitions data starting from when this device's port was last active, defaulting to ${start_date} if
  -- `port_last_active_ms` is NULL.
  WHERE ptot.date BETWEEN COALESCE(DATE(FROM_UNIXTIME(pla.port_last_active_ms / 1000)), ${start_date}) AND ${end_date}
),

pto_transition_running_sum AS (
  -- Combine PTO intervals.
  -- For example, a device has two equipments:
  --   equipment A is in use on port 1 between (10, 50),
  --   equipment B is in use on port 2 between (30, 70),
  -- we want to find the union of these intervals, i.e. (10, 70),
  -- when we calculate PTO intervals, so we don't double count
  -- duration (30, 50) when we report total ON time.
  --
  -- Perform a running sum of "transition" for each device.
  -- The total represents "number of overlapping intervals".
  -- When total=1, start an interval. When total=0, close the interval.
  SELECT
    date,
    org_id,
    device_id,
    time,
    SUM(
      CASE
        WHEN value = 1 THEN 1
        -- When the first value is not equal to 1, avoid starting the total at -1 by returning 0 instead.
        WHEN time = FIRST_VALUE(time) OVER (PARTITION BY org_id, device_id ORDER BY time) THEN 0
        ELSE -1
      END
    ) OVER (PARTITION BY org_id, device_id ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total
  FROM transitions_with_last_active
  WHERE is_productive
),

intervals AS (
  SELECT
    date,
    org_id,
    device_id,
    start_ms,
    end_ms,
    total as value
  FROM (
    SELECT
      date,
      org_id,
      device_id,
      total,
      time AS start_ms,
      COALESCE(
        LEAD(time) OVER ( PARTITION BY org_id, device_id ORDER BY time ASC),
        CAST(unix_timestamp(CAST (${end_date} AS TIMESTAMP)) * 1000 AS BIGINT)
      ) AS end_ms
    FROM (
      -- Filter open and close events.
      SELECT
        date,
        org_id,
        device_id,
        time,
        total
      FROM
        pto_transition_running_sum
      WHERE
        (total = 0 OR total = 1)
        AND date >= ${start_date}
        AND date < ${end_date}
    )
  )
),

-- combine rows where the value doesn't change and the start_ms of the row is equal to the end_ms
-- of the previous row.
packed_intervals AS (
  SELECT
    date,
    org_id,
    device_id,
    MIN(start_ms) as start_ms,
    MAX(end_ms) as end_ms,
    value
  FROM (
    -- If the start_ms of the current row = end_ms of the previous row for the same pto value, then
    -- add them to the same group.
    SELECT
      *,
      -- calculates the running sum as follows: for intervals where the value does not change and the start
      -- of the previous interval is equal to the end of the current interval, we assign the same as previous interval
      -- interval and for the next interval where the start != prev.end the sum gets incremented by 1.
      SUM(
        IF(LAG(end_ms) OVER (PARTITION BY org_id,device_id,value ORDER BY start_ms) = start_ms, 0, 1)
      ) OVER ( PARTITION BY org_id, device_id, value ORDER BY start_ms) AS interval_id
    FROM (
      -- Filter out rows where start_ms == end_ms. These rows don't add any
      -- time to a devices sum total
      SELECT *
      FROM intervals
      WHERE start_ms != end_ms
    )
  )
  GROUP BY date, org_id, device_id, value, interval_id
),

-- we split pto intervals at day boundaries so when downstream nodes query for data by
-- ${start_date} they do not miss out on intervals where the node ends on some future date.
intervals_by_covering_day AS (
  SELECT
    DATE(day_start) AS date,
    original_interval_duration_ms,
    org_id,
    device_id,
    value,
    -- start the interval on the day's start if it starts on a previous day and end the interval
    -- at the day's end if it continues into the next day.
    GREATEST(start_ms, CAST(unix_timestamp(day_start) * 1000 AS BIGINT)) AS start_ms,
    LEAST(end_ms, CAST(unix_timestamp(day_start + INTERVAL 1 DAY) * 1000 AS BIGINT)) AS end_ms
  FROM (
    -- create new row for each day the interval spans.
    SELECT
      *,
      end_ms - start_ms AS original_interval_duration_ms,
      EXPLODE(
        SEQUENCE(
          DATE_TRUNC('day', from_unixtime(start_ms / 1000)),
          DATE_TRUNC('day', from_unixtime((end_ms - 1)/ 1000)), -- SEQUENCE() takes inclusive end time.
          INTERVAL 1 day
        )
      ) AS day_start
    FROM packed_intervals
    WHERE end_ms IS NOT NULL
  )
)

SELECT
  *
FROM intervals_by_covering_day
WHERE date >= ${start_date}
AND  date < ${end_date}

