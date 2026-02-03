WITH stats_with_offsets AS (
  SELECT
    date,
    org_id,
    object_id,
    -- only support offset types MIN_IDLE_DURATION and ENGINE_ACTIVITY_TIMEOUT for now as the others are buggy
    -- limit offset times to 10 minutes. offsets should generally be brief and we want to cap possibly large mistakes in the offset
    -- also limit the corrected time to be within our date window.
    IF(value.proto_value.engine_state.offset_type IN (1,2),
        time - LEAST(COALESCE(value.proto_value.engine_state.offset_ms, 0), 600000),
        time
      ) as corrected_time,
    value.int_value AS int_value,
    value.is_databreak AS is_databreak,
    time
  FROM kinesisstats.osdenginestate
  -- Subtract a day from the start date as we might want to add a simulated OFF stat
  -- at the stat time plus 24 hours which could be the start_date.
  WHERE date >= DATE_SUB(${start_date}, 1)
    -- Add 30 days to the end date so we can assign an alternate engine state value to the last engine
    -- state reported within the timerange by evaluating the next object stat's flags which may be
    -- present after the end date.
    AND date < DATE_ADD(${end_date}, 30)
    AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
    -- Ignore any stats with time greater than or equal to now,
    -- since they are invalid
    AND time < UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) * 1000

),

with_offset_values_limited AS (
  SELECT
    TO_DATE(
      FROM_UNIXTIME(GREATEST(LAG(corrected_time) OVER (w), corrected_time) / 1000)
    ) AS date,
    org_id,
    object_id,
    time,
    -- an engine state's offset should only go as far back as the previous point
    GREATEST(LAG(corrected_time) OVER (w), corrected_time) AS corrected_time,
    int_value,
    is_databreak
  FROM stats_with_offsets
  WINDOW w AS (PARTITION BY org_id, object_id ORDER BY time ASC)
),

-- group points by corrected_time so that we can drop points that have been overlapped
-- for each group, keep the data corresponding to the most recent point (ie. the point that had the offset)
stats_without_duplicates AS (
  SELECT
    MAX_BY(date, time) as date,
    org_id,
    object_id,
    corrected_time as time,
    MAX_BY(int_value, time) as int_value,
    MAX_BY(is_databreak, time) as is_databreak
  FROM with_offset_values_limited
  GROUP BY org_id, object_id, corrected_time
),

stats_with_corrected_state AS (
  SELECT
    date,
    org_id,
    object_id,
    time,
    is_databreak,
    CASE
      -- A databreak means the previous value should be considered an "end" which corresponds with the OFF state (0).
      WHEN LEAD(is_databreak) OVER (PARTITION BY org_id, object_id ORDER BY time ASC) THEN 0
      -- Otherwise, just use the int_value.
      ELSE int_value
    END AS int_value
  FROM stats_without_duplicates
),

filtered_stats_with_corrected_state AS (
  SELECT
    date,
    org_id,
    object_id,
    time,
    int_value
  FROM stats_with_corrected_state
  WHERE NOT is_databreak
),

-- Generate stats with the next reported time (only to be used for calculating simulated stats).
stats_with_next_time AS (
  SELECT
    date,
    org_id,
    object_id,
    time,
    int_value,
    -- Ideally we would always have a next stat available however we might not if we're fetching
    -- data up until NOW and so use the next time if it is available, otherwise use the end_date if
    -- end_date is in the past.
    -- We want to avoid making assumptions about missing stats unless it is highly probable and so
    -- we don't assume a next_time if end_date is in the future.
    -- It is safe to assume that the next_time is end_date as future runs of the pipeline with
    -- more data will correct this assumption and overwrite data that has been written, if there is
    -- a long gap between stats then this assumption is also fine as we would want to have simulated
    -- an OFF stat.
    COALESCE(
      LEAD(time) OVER (PARTITION BY org_id, object_id ORDER BY time ASC),
      IF(${end_date} < CURRENT_DATE(), unix_timestamp(to_timestamp(${end_date})) * 1000, NULL)
    ) AS next_time
  FROM filtered_stats_with_corrected_state
),

-- If there is more than 24 hours between a stat and the next stat then simulate an OFF stat
-- as the engine is not reasonably expected to be ON or IDLE for more than 24 hours.
-- It is possible that we create multiple OFF stats as the window of the pipeline runs increments
-- but this is fine as we do not assume that stats are reported only on state change.
simulated_off_stats AS (
  SELECT
    TO_DATE(
      FROM_UNIXTIME((time + 86400000) / 1000)
    ) AS date,
    org_id,
    object_id,
    time + 86400000 AS time,
    INT(0) AS int_value
  FROM stats_with_next_time
  WHERE next_time IS NOT NULL
  -- 1 and 2 correspond to ON and IDLE respectively.
  AND int_value IN (1,2)
  AND (next_time - time) > 86400000
),

-- Merge the filtered corrected stats with the simulated OFF stats.
stats_with_simulated_stats AS (
  (SELECT * FROM filtered_stats_with_corrected_state)
  UNION ALL
  (SELECT * FROM simulated_off_stats)
)

SELECT
    date,
    org_id,
    object_id,
    time,
    int_value
  FROM stats_with_simulated_stats
  WHERE date >= ${start_date}
    AND date < ${end_date}
