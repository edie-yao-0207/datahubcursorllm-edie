WITH air_temp_celsius AS (
  SELECT
    org_id,
    object_id,
    value.proto_value.engine_gauge_event.air_temp_milli_c / 1000 AS air_temp_c,
    time AS time_ms,
    date
  FROM
    kinesisstats.osdenginegauge
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
    AND value.is_end = false
    AND value.is_databreak = false
    AND value.proto_value.engine_gauge_event.air_temp_milli_c IS NOT NULL
),
ranked_air_temp AS (
  SELECT
    espfi.date,
    espfi.org_id,
    espfi.object_id,
    espfi.engine_state,
    espfi.productive_pto_state,
    espfi.fuel_consumed_ml,
    espfi.gaseous_fuel_consumed_grams,
    espfi.start_ms,
    espfi.end_ms,
    espfi.original_engine_state_interval_duration_ms,
    espfi.original_fuel_interval_duration_ms,
    espfi.original_gaseous_consumption_interval_duration_ms,
    espfi.original_pto_interval_duration_ms,
    atc.air_temp_c,
    RANK() OVER (
      PARTITION BY espfi.org_id, espfi.object_id, espfi.date, espfi.start_ms, espfi.end_ms
      ORDER BY atc.time_ms ASC
    ) AS temp_rank
  FROM
    engine_state_report.engine_state_pto_fuel_intervals espfi
  LEFT JOIN air_temp_celsius atc
    ON atc.date = espfi.date
    AND atc.org_id = espfi.org_id
    AND atc.object_id = espfi.object_id
    AND atc.time_ms >= espfi.start_ms
    AND atc.time_ms < espfi.end_ms
  WHERE
    espfi.date >= ${start_date}
    AND espfi.date < ${end_date}
),
combined_intervals_air_temp AS (
  SELECT
    date,
    org_id,
    object_id,
    engine_state,
    productive_pto_state,
    fuel_consumed_ml,
    gaseous_fuel_consumed_grams,
    start_ms,
    end_ms,
    original_engine_state_interval_duration_ms,
    original_fuel_interval_duration_ms,
    original_gaseous_consumption_interval_duration_ms,
    original_pto_interval_duration_ms,
    AVG(air_temp_c) AS avg_first_5_air_temp_c -- We have observed spikes when a vehicle starts and goes to idling directly. So we are taking an average of the first 5 temperatures in the interval to soften the slope.
  FROM
    ranked_air_temp
  WHERE
    temp_rank <= 5
  GROUP BY
    date,
    org_id,
    object_id,
    engine_state,
    productive_pto_state,
    fuel_consumed_ml,
    gaseous_fuel_consumed_grams,
    start_ms,
    end_ms,
    original_engine_state_interval_duration_ms,
    original_fuel_interval_duration_ms,
    original_gaseous_consumption_interval_duration_ms,
    original_pto_interval_duration_ms
)

SELECT
  date,
  org_id,
  object_id,
  engine_state,
  productive_pto_state,
  fuel_consumed_ml,
  gaseous_fuel_consumed_grams,
  start_ms,
  end_ms,
  original_engine_state_interval_duration_ms,
  original_fuel_interval_duration_ms,
  original_gaseous_consumption_interval_duration_ms,
  original_pto_interval_duration_ms,
  CASE
    WHEN avg_first_5_air_temp_c IS NULL THEN 9999 -- Unknown values.
    WHEN avg_first_5_air_temp_c < -20 THEN 9999
    WHEN avg_first_5_air_temp_c > 50 THEN 9999
    ELSE FLOOR(avg_first_5_air_temp_c)
  END AS avg_air_temp_c
FROM
  combined_intervals_air_temp
