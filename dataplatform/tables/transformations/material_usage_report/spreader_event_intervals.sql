WITH spreader_state AS (
  SELECT
    org_id,
    object_id AS device_id,
    DATE(date) AS date,
    time,
    CASE
      WHEN value.int_value = 1
      and not value.is_end
      and not value.is_databreak THEN "on"
      ELSE "off"
    END AS state
  FROM
    kinesisstats.osdsaltspreaderstateactivelyspreading
  WHERE
    date >= ${start_date}
    and date < ${end_date}
),
spreader_transitions AS (
  SELECT
    *
  FROM
    (
      SELECT
        org_id,
        device_id,
        state AS current_state,
        time AS current_time,
        LEAD(state) OVER (
          PARTITION by org_id,
          device_id
          ORDER BY
            time ASC,
            state ASC
        ) AS next_state,
        LEAD(time) OVER (
          PARTITION by org_id,
          device_id
          ORDER BY
            TIME ASC,
            state ASC
        ) AS next_time
      FROM
        spreader_state
    )
  WHERE
    current_state != next_state
    OR next_state IS NULL
),
-- /*
-- To build the correct start time for each spreader event
-- interval, join each state change row with last end state.
-- Use the last end state's timestamp as the start timestamp
-- for a given event interval if current state is the same
-- as the previous state.
-- If we have an open ON interval for which we have yet to
-- receive a spreader OFF for, set the spreader end time to
-- time now so that we can compute ongoing material usage
-- without waiting for the spreader OFF stat.
-- */
spreader_state_intervals AS (
  SELECT
    org_id,
    device_id,
    current_state,
    CASE
      WHEN next_state IS NULL then "off"
      ELSE next_state
    END as next_state,
    CASE
      WHEN prev_state = current_state THEN prev_time
      ELSE current_time
    END AS spreader_start_ms,
    CASE
      WHEN next_time IS NOT NULL then next_time
      ELSE array_min(array(unix_timestamp(current_timestamp()) * 1000, unix_timestamp(date(${end_date}))* 1000))
    END AS spreader_end_ms
  FROM
    (
      SELECT
        org_id,
        device_id,
        LAG(next_state) OVER (
          PARTITION by org_id,
          device_id
          ORDER BY
            current_time ASC
        ) AS prev_state,
        LAG(next_time) OVER (
          PARTITION by org_id,
          device_id
          ORDER BY
            current_time ASC
        ) AS prev_time,
        current_state,
        current_time,
        next_state,
        next_time
      FROM
        spreader_transitions
    )
),
on_intervals as (
  SELECT
    date(from_unixtime(spreader_start_ms / 1000)) date,
    org_id,
    device_id,
    spreader_start_ms,
    spreader_end_ms,
    -- Mode = 0 indicates the spreader was on.
    -- This is currently not used for anything downstream
    -- but could be used in future to indicate blast mode vs.
    -- normal operation.
    0 mode
  from
    spreader_state_intervals
  where
    current_state = "on"
    and next_state = "off"
)
select
  *
from
  on_intervals
where
  date >= ${start_date}
  and date < ${end_date}
