WITH gaseous_stats AS (
  SELECT
    *
  FROM engine_state_report.gaseous_fuel_intervals
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
),

engine_state_fuel_intervals AS (
  SELECT
    *
  FROM engine_state_report.engine_state_fuel_intervals
  WHERE date >= DATE_SUB(${start_date}, 1)
  AND date < ${end_date}
),

engine_state_with_gaseous_consumption AS (
  SELECT
    COALESCE(ei.date, gi.date) AS date,
    COALESCE(ei.org_id, gi.org_id) AS org_id,
    COALESCE(ei.object_id, gi.object_id) AS object_id,
    COALESCE(gi.gaseous_fuel_consumed_grams, 0) AS gaseous_fuel_consumed_grams,
    gi.start_ms AS gi_start_ms,
    -- If there were gaseous fuel intervals where no engine state overlaps we still want to add them
    COALESCE(ei.start_ms,  gi.start_ms) AS ei_start_ms,
    COALESCE(ei.end_ms,  gi.end_ms) AS ei_end_ms,
    -- While we expect to have an engine state for every point in time, in case we do not have engine
    -- state interval, we mark it as OFF
    COALESCE(ei.engine_state, 'OFF') AS engine_state,
    GREATEST(gi.start_ms, ei.start_ms) AS subset_start_ms,
    LEAST(gi.end_ms, ei.end_ms) AS subset_end_ms,
    original_engine_state_interval_duration_ms,
    original_fuel_interval_duration_ms,
    gi.original_interval_duration_ms AS original_gaseous_consumption_interval_duration_ms
  FROM engine_state_fuel_intervals AS ei
  -- Join with fuel_intervals_in_range so that we do not miss out on any fuel intervals
  FULL JOIN gaseous_stats AS gi
    ON gi.org_id = ei.org_id
    AND gi.object_id = ei.object_id
    -- Overlapping
    AND gi.start_ms < ei.end_ms
    AND gi.end_ms > ei.start_ms
),

-- Calculate the interval length so that we can spread data proportionally to it
intervals_with_lengths AS (
  SELECT
    *,
    -- We only want to attribute gaseous fuel consumption to an overlapping OFF interval if the fuel interval does not also overlap with any ON or IDLE intervals
    -- So if this row is an OFF engine state, we check all rows that this row's original fuel interval overlaps with,
    -- if any of them are ON or IDLE then we give this interval a length of 0 so that no fuel consumption is distributed to this OFF engine interval later
    IF(engine_state = "OFF"
      AND MAX(IF(engine_state = "OFF", 0, 1)) OVER (PARTITION BY org_id, object_id, gi_start_ms) > 0,
    0,
    subset_end_ms - subset_start_ms
  ) as interval_length
  FROM engine_state_with_gaseous_consumption
),

-- Spread data proportionally to the interval length. This will also reduce number of rows
-- to combine with the PTO intervals.
states_with_distributed_gaseous_fuel AS (
  SELECT
    date,
    org_id,
    object_id,
    ei_start_ms,
    ei_end_ms,
    gi_start_ms,
    engine_state,
    gaseous_fuel_consumed_grams * interval_length / (
      SUM(interval_length) OVER (PARTITION BY org_id, object_id, gi_start_ms)
    ) AS gaseous_fuel_consumed_grams,
    original_engine_state_interval_duration_ms,
    original_fuel_interval_duration_ms,
    original_gaseous_consumption_interval_duration_ms
  FROM intervals_with_lengths
),

engine_state_intervals_gaseous_consumption AS (
  SELECT
  date,
  org_id,
  object_id,
  ei_start_ms as start_ms,
  ei_end_ms as end_ms,
  engine_state,
  -- We wrap gaseous_fuel_consumed_grams in coalesce since 0/0 operation may give us nulls.
  SUM(COALESCE(gaseous_fuel_consumed_grams, 0)) AS gaseous_fuel_consumed_grams,
  MIN(original_engine_state_interval_duration_ms) AS original_engine_state_interval_duration_ms,
  MIN(original_gaseous_consumption_interval_duration_ms) AS original_gaseous_consumption_interval_duration_ms
FROM states_with_distributed_gaseous_fuel
-- Sum up fuel consumption for each engine state interval
WHERE
  date >= ${start_date}
  AND date < ${end_date}
GROUP BY date, org_id, object_id, ei_start_ms, ei_end_ms, engine_state
),

-- Match the engine state intervals with gaseous consumption back to those with the fuel intervals.
-- This is a left join as the gaseous consumption intervals are produced from the fuel intervals, so should be a superset.
-- However may not match exactly as additional gaseous consumption intervals may be produced where there is no intermediate engine state/fuel interval.
engine_state_intervals_with_fuel_and_gaseous_consumption AS (
 SELECT
    gaseous_intervals.date AS date,
    gaseous_intervals.org_id AS org_id,
    gaseous_intervals.object_id AS object_id,
    gaseous_intervals.start_ms AS start_ms,
    gaseous_intervals.end_ms AS end_ms,
    gaseous_intervals.engine_state AS engine_state,
    gaseous_intervals.gaseous_fuel_consumed_grams AS gaseous_fuel_consumed_grams,
    gaseous_intervals.original_engine_state_interval_duration_ms AS original_engine_state_interval_duration_ms,
    gaseous_intervals.original_gaseous_consumption_interval_duration_ms AS original_gaseous_consumption_interval_duration_ms,
    COALESCE(fuel_intervals.fuel_consumed_ml, 0) AS fuel_consumed_ml,
    fuel_intervals.original_fuel_interval_duration_ms AS original_fuel_interval_duration_ms
  FROM engine_state_intervals_gaseous_consumption AS gaseous_intervals
  LEFT JOIN engine_state_fuel_intervals AS fuel_intervals ON
    gaseous_intervals.date = fuel_intervals.date
    AND gaseous_intervals.org_id = fuel_intervals.org_id
    AND gaseous_intervals.object_id = fuel_intervals.object_id
    AND gaseous_intervals.start_ms = fuel_intervals.start_ms
    AND gaseous_intervals.end_ms = fuel_intervals.end_ms
    AND gaseous_intervals.engine_state = fuel_intervals.engine_state
)

SELECT
  date,
  org_id,
  object_id,
  start_ms,
  end_ms,
  engine_state,
  fuel_consumed_ml,
  gaseous_fuel_consumed_grams,
  original_engine_state_interval_duration_ms,
  original_fuel_interval_duration_ms,
  original_gaseous_consumption_interval_duration_ms
FROM engine_state_intervals_with_fuel_and_gaseous_consumption
WHERE
  date >= ${start_date}
  AND date < ${end_date}
