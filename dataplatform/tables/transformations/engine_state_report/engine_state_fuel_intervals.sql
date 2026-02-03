WITH fuel_stats AS (
 SELECT
    *
  FROM engine_state_report.fuel_intervals
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
),

engine_intervals AS (
  SELECT
    *
  FROM engine_state.intervals
  WHERE date >= DATE_SUB(${start_date}, 1)
  AND date < ${end_date}
),

engine_state_with_fuel AS (
  SELECT
    COALESCE(ei.date, fi.date) AS date,
    COALESCE(ei.org_id, fi.org_id) AS org_id,
    COALESCE(ei.object_id, fi.object_id) AS object_id,
    COALESCE(fi.fuel_consumed_ml, 0) AS fuel_consumed_ml,
    fi.start_ms AS fi_start_ms,
    -- If there were fuel intervals where no engine state overlaps we still want to add them
    COALESCE(ei.start_ms,  fi.start_ms) AS ei_start_ms,
    COALESCE(ei.end_ms,  fi.end_ms) AS ei_end_ms,
    -- While we expect to have an engine state for every point in time, in case we do not have engine
    -- state interval, we mark it as OFF
    COALESCE(ei.state, 'OFF') AS engine_state,
    GREATEST(fi.start_ms, ei.start_ms) AS subset_start_ms,
    LEAST(fi.end_ms, ei.end_ms) AS subset_end_ms,
    ei.original_interval_duration_ms AS original_engine_state_interval_duration_ms,
    fi.original_interval_duration_ms AS original_fuel_interval_duration_ms
  FROM engine_intervals AS ei
  -- Join with fuel_intervals_in_range so that we do not miss out on any fuel intervals
  FULL JOIN fuel_stats AS fi
    ON fi.org_id = ei.org_id
    AND fi.object_id = ei.object_id
    -- Overlapping
    AND fi.start_ms < ei.end_ms
    AND fi.end_ms > ei.start_ms
),

-- Calculate the interval length so that we can spread data proportionally to it
intervals_with_lengths AS (
  SELECT
    *,
    -- We only want to attribute fuel consumption to an overlapping OFF interval if the fuel interval does not also overlap with any ON or IDLE intervals
    -- So if this row is an OFF engine state, we check all rows that this row's original fuel interval overlaps with,
    -- if any of them are ON or IDLE then we give this interval a length of 0 so that no fuel consumption is distributed to this OFF engine interval later
    IF(engine_state = "OFF"
      AND MAX(IF(engine_state = "OFF", 0, 1)) OVER (PARTITION BY org_id, object_id, fi_start_ms) > 0,
    0,
    subset_end_ms - subset_start_ms
  ) as interval_length
  FROM engine_state_with_fuel
),

-- Spread data proportionally to the interval length. This will also reduce number of rows
-- to combine with the PTO intervals.
states_with_distributed_fuel AS (
  SELECT
    date,
    org_id,
    object_id,
    ei_start_ms,
    ei_end_ms,
    fi_start_ms,
    engine_state,
    fuel_consumed_ml * interval_length / (
      SUM(interval_length) OVER (PARTITION BY org_id, object_id, fi_start_ms)
    ) AS fuel_consumed_ml,
    original_engine_state_interval_duration_ms,
    original_fuel_interval_duration_ms
  FROM intervals_with_lengths
)

SELECT
  date,
  org_id,
  object_id,
  ei_start_ms as start_ms,
  ei_end_ms as end_ms,
  engine_state,
  -- We wrap fuel_consumed_ml in coalesce since 0/0 operation may give us nulls.
  SUM(COALESCE(fuel_consumed_ml, 0)) AS fuel_consumed_ml,
  MIN(original_engine_state_interval_duration_ms) AS original_engine_state_interval_duration_ms,
  MIN(original_fuel_interval_duration_ms) AS original_fuel_interval_duration_ms
FROM states_with_distributed_fuel
-- Sum up fuel consumption for each engine state interval
WHERE
  date >= ${start_date}
  AND date < ${end_date}
GROUP BY date, org_id, object_id, ei_start_ms, ei_end_ms, engine_state
