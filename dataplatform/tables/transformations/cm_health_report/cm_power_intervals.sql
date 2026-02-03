WITH cm_power_states AS (
  SELECT
    cm_linked_vgs.org_id,
    cm_linked_vgs.linked_cm_id,
    cm_linked_vgs.vg_device_id AS device_id,
    osdpowerstate.time AS timestamp,
    osdpowerstate.date AS date,
    CASE
      WHEN osdpowerstate.value.int_value == 1 THEN true
      ELSE false
    END AS has_power
  FROM
    cm_health_report.cm_2x_3x_linked_vgs cm_linked_vgs
    LEFT JOIN kinesisstats.osdpowerstate osdpowerstate ON cm_linked_vgs.org_id = osdpowerstate.org_id
    AND cm_linked_vgs.linked_cm_id = osdpowerstate.object_id
  WHERE
    osdpowerstate.value.is_databreak = false
    AND osdpowerstate.value.is_end = false
    AND cm_linked_vgs.linked_cm_id IS NOT NULL
    -- Always use the most recent 30 days of data.
    AND osdpowerstate.date < ${end_date}
    AND osdpowerstate.date >= ${start_date}
    -- Exclude data in the past 4 hours due to ingestion delays.
    AND osdpowerstate.time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
),
cm_power_lag AS (
  -- In order to build our transitions, we need to construct the start and end time.
  -- Grab prior state and prior time for each row.
  SELECT
    linked_cm_id,
    device_id,
    org_id,
    lag(timestamp) OVER (
      PARTITION BY org_id,
      linked_cm_id,
      device_id
      ORDER BY
        timestamp
    ) AS prev_time,
    lag(has_power) OVER (
      PARTITION BY org_id,
      linked_cm_id,
      device_id
      ORDER BY
        timestamp
    ) AS prev_state,
    timestamp AS cur_time,
    has_power AS cur_state,
    date
  FROM
    cm_power_states
),
cm_power_hist AS (
  -- Filter down the rows to only include state changes.
  SELECT
    *
  FROM
    cm_power_lag lag
  WHERE
    lag.prev_state != lag.cur_state
    OR lag.prev_state IS NULL
),
power_states AS (
  -- Since we only grabbed the transitions, we don't see
  -- the timestamps for consecutive reported objectStat values. To
  -- create accurate intervals, we need to grab the next rows cur_time
  -- since that timestamp is the end of the consecutive values (i.e. state transition).
  SELECT
    org_id,
    device_id,
    linked_cm_id,
    cur_time AS start_ms,
    cur_state AS has_power,
    lead(cur_time) OVER (
      PARTITION BY org_id,
      linked_cm_id,
      device_id
      ORDER BY
        cur_time
    ) AS end_ms,
    date
  FROM
    cm_power_hist
) -- Create final view that have both unpowered and powered ranges
SELECT
  *
FROM
  power_states
WHERE
  end_ms IS NOT NULL
