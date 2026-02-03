WITH fuel_with_ecu_segments AS (
  SELECT
    fuel.*,
    seg.segment_start
  FROM
    engine_state_report.engine_state_pto_fuel_intervals fuel
    LEFT JOIN fuel_energy_efficiency_report.ecu_filter_segments seg
      ON fuel.start_ms < seg.segment_end
      AND fuel.end_ms > seg.segment_start
      AND seg.device_id = fuel.object_id
      AND seg.org_id = fuel.org_id
  WHERE
    fuel.date >= ${start_date}
    and fuel.date <= ${end_date}
),

-- Since a fuel interval can overlap with multiple spotty ECU segments
-- we need to consolidate any rows that were duplicated in the above join.
-- The only data that will be different between these duplicates is segment_start
-- and if any of the segment_starts is not null, we know an overlap exists and can set fuel to 0
filtered_fuel AS (
  SELECT
    date,
    org_id,
    object_id,
    start_ms,
    end_ms,
    IF(SOME(segment_start IS NOT NULL), 0, fuel_consumed_ml) as fuel_consumed_ml,
    IF(SOME(segment_start IS NOT NULL), 0, gaseous_fuel_consumed_grams) as gaseous_fuel_consumed_grams,
    engine_state,
    productive_pto_state
  FROM fuel_with_ecu_segments
  GROUP BY date, org_id, object_id, start_ms, end_ms, engine_state, productive_pto_state, fuel_consumed_ml, gaseous_fuel_consumed_grams
),

-- Expand engine state intervals into hour intervals that cover the interval
engine_state_intervals_by_covering_hour AS (
  SELECT
    date,
    org_id,
    object_id,
    productive_pto_state,
    engine_state,
    start_ms AS original_start_ms,
    end_ms AS original_end_ms,
    fuel_consumed_ml,
    gaseous_fuel_consumed_grams,
    EXPLODE(
      -- SEQUENCE() end time is inclusive, subtract 1 ms so that an interval ending on an hour boundary does not EXPLODE into that following hour
      SEQUENCE(
        DATE_TRUNC('hour', from_unixtime(start_ms / 1000)),
        DATE_TRUNC('hour', from_unixtime((end_ms - 1)/ 1000)),
        INTERVAL 1 hour
      )
    ) AS covering_hour_start
  FROM filtered_fuel
  WHERE date >= ${start_date}
    AND date < ${end_date}
),

-- Distribute fuel consumption to each interval proportional to its length
distributed_fuel_intervals AS (
  SELECT
    date,
    org_id,
    object_id,
    subset_start_ms AS start_ms,
    subset_end_ms AS end_ms,
    fuel_consumed_ml * interval_length / (
      SUM(interval_length) OVER (PARTITION BY org_id, object_id, original_start_ms)
    ) AS fuel_consumed_ml,
    gaseous_fuel_consumed_grams * interval_length / (
      SUM(interval_length) OVER (PARTITION BY org_id, object_id, original_start_ms)
    ) AS gaseous_fuel_consumed_grams,
    productive_pto_state,
    engine_state
   FROM (
     SELECT
        DATE(covering_hour_start) AS date,
        org_id,
        object_id,
        fuel_consumed_ml,
        gaseous_fuel_consumed_grams,
        original_start_ms,
        original_end_ms,
        productive_pto_state,
        engine_state,
        GREATEST(UNIX_TIMESTAMP(covering_hour_start) * 1000, original_start_ms) AS subset_start_ms,
        LEAST(UNIX_TIMESTAMP(covering_hour_start + interval 1 hour) * 1000, original_end_ms) AS subset_end_ms,
        LEAST(UNIX_TIMESTAMP(covering_hour_start + interval 1 hour) * 1000, original_end_ms) - GREATEST(UNIX_TIMESTAMP(covering_hour_start) * 1000, original_start_ms) AS interval_length,
        covering_hour_start
     FROM engine_state_intervals_by_covering_hour
   )
),

join_driver_assignments AS (
  SELECT
    fi.date,
    fi.org_id,
    fi.object_id,
    fi.start_ms,
    fi.end_ms,
    fi.fuel_consumed_ml,
    fi.gaseous_fuel_consumed_grams,
    fi.productive_pto_state,
    fi.engine_state,
    COALESCE(driver.driver_id, 0) AS driver_id,
    driver.start_time AS driver_start_time,
    driver.end_time AS driver_end_time
  FROM distributed_fuel_intervals fi
  LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS driver
    ON fi.org_id = driver.org_id
    AND fi.object_id = driver.device_id
    AND fi.start_ms < driver.end_time
    AND fi.end_ms > driver.start_time
),

-- Distribute fuel consumption and engine state durations to each driver assignment, proportional to its length
distributed_fuel_intervals_across_driver_assignments AS (
  SELECT
    date,
    org_id,
    object_id,
    driver_id,
    subset_start_ms AS start_ms,
    subset_end_ms AS end_ms,
    productive_pto_state,
    engine_state,
    fuel_consumed_ml * interval_length / (
      SUM(interval_length) OVER (PARTITION BY org_id, object_id, original_start_ms)
    ) AS fuel_consumed_ml,
    gaseous_fuel_consumed_grams * interval_length / (
      SUM(interval_length) OVER (PARTITION BY org_id, object_id, original_start_ms)
    ) AS gaseous_fuel_consumed_grams
   FROM (
     SELECT
        date,
        org_id,
        object_id,
        driver_id,
        start_ms AS original_start_ms,
        end_ms AS original_end_ms,
        fuel_consumed_ml,
        gaseous_fuel_consumed_grams,
        GREATEST(driver_start_time, start_ms) AS subset_start_ms,
        LEAST(driver_end_time, end_ms) AS subset_end_ms,
        LEAST(driver_end_time, end_ms) - GREATEST(driver_start_time, start_ms) AS interval_length,
        productive_pto_state,
        engine_state
     FROM join_driver_assignments
   )
)

-- Sum fuel and engine state durations by (device, driver, hour)
SELECT
  ed.date,
  ed.org_id,
  ed.object_id,
  COALESCE(ed.driver_id, 0) AS driver_id,
  DATE_TRUNC('hour', FROM_UNIXTIME(ed.start_ms / 1000)) AS interval_start,
  DATE_TRUNC('hour', FROM_UNIXTIME(ed.start_ms / 1000)) + interval 1 hour AS interval_end,
  SUM(
    CASE
      WHEN ed.engine_state = "ON" OR ed.engine_state = "IDLE" THEN
        ed.end_ms - ed.start_ms
      ELSE 0
    END
  ) AS on_duration_ms,
  SUM(
    CASE
      WHEN ed.engine_state = "IDLE" THEN
        ed.end_ms - ed.start_ms
      ELSE 0
    END
  ) AS idle_duration_ms,
  SUM(
    CASE
      WHEN ed.engine_state = "IDLE" AND productive_pto_state = 1 THEN
        ed.end_ms - ed.start_ms
      ELSE 0
    END
  ) AS aux_during_idle_ms,
  -- if > 100 gallons is consumed in an hour, we can assume the data is bad and zero it out
  CAST(IF(SUM(fuel_consumed_ml) < 454609, SUM(fuel_consumed_ml), 0) AS LONG) AS fuel_consumed_ml,
  -- We can apply the same logic to gaseous fuel. 100 gallons of diesel is equivalent to ~271 kg of gaseous fuel. 1kg of gaseous fuel = 50MJ. 1 liter of diesel = 35.8MJ.
  CAST(IF(SUM(gaseous_fuel_consumed_grams) < 271000, SUM(gaseous_fuel_consumed_grams), 0) AS LONG) AS gaseous_fuel_consumed_grams
FROM distributed_fuel_intervals_across_driver_assignments ed
WHERE ed.date >= ${start_date}
  AND ed.date < ${end_date}
GROUP BY ed.date, ed.org_id, ed.object_id, COALESCE(ed.driver_id, 0), DATE_TRUNC('hour', FROM_UNIXTIME(ed.start_ms / 1000))
HAVING on_duration_ms != 0 OR idle_duration_ms != 0 OR aux_during_idle_ms != 0 OR fuel_consumed_ml != 0 OR gaseous_fuel_consumed_grams != 0


