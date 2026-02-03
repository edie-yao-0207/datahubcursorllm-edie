WITH combined_engine_pto_intervals_with_total_fuel AS (
  SELECT
    esf.date,
    esf.org_id,
    esf.object_id,
    esf.fuel_consumed_ml,
    esf.gaseous_fuel_consumed_grams,
    esf.start_ms AS es_start_ms,
    esf.end_ms AS es_end_ms,
    esf.engine_state,
    COALESCE(pp.value, 0) as productive_pto_state,
    GREATEST(esf.start_ms, COALESCE(pp.start_ms, esf.start_ms)) AS subset_start_ms,
    LEAST(esf.end_ms, COALESCE(pp.end_ms, esf.end_ms)) AS subset_end_ms,
    original_engine_state_interval_duration_ms,
    original_fuel_interval_duration_ms,
    original_gaseous_consumption_interval_duration_ms,
    pp.original_interval_duration_ms AS original_pto_interval_duration_ms
  FROM engine_state_report.engine_state_fuel_gaseous_intervals esf
  LEFT OUTER JOIN pto_input.productive_pto pp
    ON esf.date = pp.date
    AND esf.org_id = pp.org_id
    AND esf.object_id = pp.device_id
    AND esf.start_ms < pp.end_ms
    AND esf.end_ms > pp.start_ms
  WHERE esf.date >= ${start_date}
    AND esf.date < ${end_date}
),

min_idle_duration_configs AS (
  SELECT
    org_id,
    object_id,
    value.proto_value.engine_state.offset_ms AS min_idle_duration_ms,
    time AS time_ms
  FROM
    kinesisstats.osdenginestate
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
    AND value.proto_value.engine_state.offset_type = 1 -- MIN_IDLE_DURATION
),

intervals_with_min_idle_duration_config AS (
  SELECT
    intervals.*,
    COALESCE(configs.min_idle_duration_ms, 0) AS min_idle_duration_ms, -- Default to 0 if no config found
    ROW_NUMBER() OVER(
      PARTITION BY intervals.org_id,
      intervals.object_id,
      intervals.subset_start_ms
      ORDER BY
        configs.time_ms ASC
    ) AS row_number
  FROM
    combined_engine_pto_intervals_with_total_fuel intervals
    LEFT JOIN min_idle_duration_configs configs ON configs.org_id = intervals.org_id
    AND configs.object_id = intervals.object_id
    AND configs.time_ms >= intervals.es_start_ms
    -- AND configs.time_ms < intervals.es_end_ms
    -- engine_state.invervals splits the intervals at midnight,
    -- so we can't guarantee that a config can be found before the end of an interval.
),

intervals_with_min_idle_duration AS (
  SELECT
    *
  FROM
    intervals_with_min_idle_duration_config
  WHERE
    row_number = 1
    AND (
      engine_state != "IDLE"
      OR subset_end_ms - subset_start_ms >= min_idle_duration_ms
    )
),

-- filter out data where fuel consumed is less than 1 gallon (3785.41 ml) in 24 hours or more if engine state is ON or IDLE
filtered_combined_engine_pto_intervals_with_total_fuel AS (
  SELECT
    *
  FROM
    intervals_with_min_idle_duration
  WHERE
    NOT (
      fuel_consumed_ml < 3785.41
      -- Taking 1ml of diesel as equivalent to 0.0358 MJ energy.
      -- And 1kg of CNG/LNG/RNG as equivalent to 50 MJ energy.
      AND gaseous_fuel_consumed_grams < 2710 -- 3785.41 ml of diesel is equivalent to 2.71 kg or 2710 grams of gaseous fuel.
      -- EVEC-2486: Filter events of more than a day. This can because the vehicle lost connectivity or the vehicle is reporting the signals wrong.
      AND (subset_end_ms - subset_start_ms >= 86400000 OR COALESCE(original_engine_state_interval_duration_ms, 0) >= 86400000)
      AND engine_state in ("ON", "IDLE")
    )
)

-- distribute fuel consumption over each event based on event length
SELECT
  date,
  org_id,
  object_id,
  engine_state,
  productive_pto_state,
  fuel_consumed_ml * (subset_end_ms - subset_start_ms) / (es_end_ms - es_start_ms) AS fuel_consumed_ml,
  gaseous_fuel_consumed_grams * (subset_end_ms - subset_start_ms) / (es_end_ms - es_start_ms) AS gaseous_fuel_consumed_grams,
  subset_start_ms AS start_ms,
  subset_end_ms AS end_ms,
  original_engine_state_interval_duration_ms,
  original_fuel_interval_duration_ms,
  original_gaseous_consumption_interval_duration_ms,
  original_pto_interval_duration_ms
FROM
  filtered_combined_engine_pto_intervals_with_total_fuel
WHERE
  date >= ${start_date}
  AND date < ${end_date}

