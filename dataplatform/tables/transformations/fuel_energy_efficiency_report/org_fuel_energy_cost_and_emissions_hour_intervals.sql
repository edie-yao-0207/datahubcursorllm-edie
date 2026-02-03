-- Calculate the detailed intervals
WITH dynamic_fuel_cost_intervals_with_interval_ends AS (
  SELECT
    fuel_cost,
    configurable_fuel_id AS fuel_id,
    locale_id,
    from_unixtime(updated_at / 1000) AS interval_start,
    COALESCE(
      from_unixtime(
        (
          LEAD(updated_at) OVER (
            PARTITION BY (locale_id, configurable_fuel_id)
            ORDER BY
              updated_at ASC
          )
        ) / 1000
      ),
      CAST (${end_date} AS TIMESTAMP)
    ) AS interval_end
  FROM
    localedb.locale_fuel_costs
),
latest_dynamic_fuel_cost_start_ms_within_hour AS (
  SELECT
    locale_id,
    fuel_id,
    MAX(interval_start) AS oldest_start_ms
  FROM
    dynamic_fuel_cost_intervals_with_interval_ends
  GROUP BY
    locale_id,
    fuel_id,
    DATE_TRUNC('hour', interval_start)
),
effective_dynamic_fuel_cost_intervals_with_interval_ends AS (
  SELECT
    dfc.fuel_cost,
    dfc.fuel_id,
    dfc.locale_id,
    dfc.interval_start,
    dfc.interval_end
  FROM
    latest_dynamic_fuel_cost_start_ms_within_hour latest
    INNER JOIN dynamic_fuel_cost_intervals_with_interval_ends dfc ON latest.locale_id = dfc.locale_id
    AND latest.fuel_id = dfc.fuel_id
    AND latest.oldest_start_ms = dfc.interval_start
),
-- Explode into hourly intervals
dynamic_fuel_cost_hour_intervals AS (
  SELECT
    interval_start,
    interval_start + INTERVAL 1 HOUR AS interval_end,
    fuel_cost AS fuel_cost_per_unit,
    fuel_id,
    locale_id
  FROM
    (
      SELECT
        fuel_cost,
        fuel_id,
        locale_id,
        EXPLODE(
          SEQUENCE(
            DATE_TRUNC('hour', interval_start),
            DATE_TRUNC('hour', interval_end - INTERVAL 1 HOUR), -- SEQUENCE() takes inclusive end time.
            INTERVAL 1 hour
          )
        ) AS interval_start
      FROM
        effective_dynamic_fuel_cost_intervals_with_interval_ends
      WHERE
        interval_start < interval_end
        AND interval_end >= ${start_date}
    )
    WHERE DATE(interval_start) >= ${start_date}
    AND DATE(interval_start) < ${end_date}
),
org_locales AS (
  SELECT
    id AS org_id,
    locale AS locale_id
  FROM
    clouddb.organizations
),
-- Setup our mappings for default emissions factors
default_emissions_factors_liters AS (
  SELECT
    *
  FROM
  VALUES
    (2319, 0), -- Gasoline
    (2697, 1), -- Diesel
    (2496, 2), -- Biodiesel
    (1655, 3), -- Flexible Fuel (E85)
    (1519, 5),  -- E100
    (2496, 6),  -- Renewable Diesel
    (1500, 10) -- LPG

    AS tab(emissions_grams_per_liter, fuel_id)
),
default_emissions_factors_kg AS (
  SELECT
    *
  FROM
  VALUES
    (1209, 8), -- CNG
    (2642, 9), -- LNG
    (1209, 11) -- RNG

    AS tab(emissions_grams_per_kg, fuel_id)
),
default_emssions_factors AS (
  SELECT
    COALESCE(fl.fuel_id, fkg.fuel_id) AS fuel_id,
    emissions_grams_per_liter,
    emissions_grams_per_kg AS emissions_grams_per_kg
  FROM
  default_emissions_factors_liters fl
  FULL JOIN default_emissions_factors_kg fkg ON fl.fuel_id = fkg.fuel_id
),
default_emissions_per_org AS (
  SELECT
    org_id,
    fuel_id,
    (emissions_grams_per_liter / 1000) / uc.gallon_per_ml AS emissions_grams_per_gallon,
    emissions_grams_per_kg AS emissions_grams_per_kg
  FROM
    org_locales
    CROSS JOIN default_emssions_factors
    CROSS JOIN helpers.unit_conversion uc
),
-- Join with org locales to create hourly dynamic fuel cost intervals per org
org_dynamic_fuel_cost_hour_intervals AS (
  SELECT
    intervals.*,
    org_locales.org_id
  FROM
    dynamic_fuel_cost_hour_intervals AS intervals
    INNER JOIN org_locales ON LOWER(org_locales.locale_id) = LOWER(intervals.locale_id)
),
-- Again repeat with custom fuel costs/emissions
custom_fuel_cost_intervals_with_interval_ends AS (
  SELECT
    org_id,
    fuel_id,
    fuel_cost_per_unit,
    emissions_grams_per_unit,
    from_unixtime(updated_at / 1000) AS interval_start,
    COALESCE(
      from_unixtime(
        (
          LEAD(updated_at) OVER (
            PARTITION BY (org_id, fuel_id)
            ORDER BY
              updated_at ASC
          )
        ) / 1000
      ),
      CAST (${end_date} AS TIMESTAMP)
    ) AS interval_end
  FROM
    fueldb_shards.custom_fuel_cost_emissions
),
latest_custom_fuel_cost_start_ms_within_hour AS (
  SELECT
    org_id,
    fuel_id,
    MAX(interval_start) AS oldest_start_ms
  FROM
    custom_fuel_cost_intervals_with_interval_ends
  GROUP BY
    org_id,
    fuel_id,
    DATE_TRUNC('hour', interval_start)
),
effective_custom_fuel_cost_intervals_with_interval_ends AS (
  SELECT
    cfc.org_id,
    cfc.fuel_id,
    cfc.fuel_cost_per_unit,
    cfc.emissions_grams_per_unit,
    cfc.interval_start,
    cfc.interval_end
  FROM
    latest_custom_fuel_cost_start_ms_within_hour latest
    INNER JOIN custom_fuel_cost_intervals_with_interval_ends cfc ON latest.org_id = cfc.org_id
    AND latest.fuel_id = cfc.fuel_id
    AND latest.oldest_start_ms = cfc.interval_start
),
custom_fuel_cost_hour_intervals AS (
  SELECT
    interval_start,
    interval_start + INTERVAL 1 HOUR AS interval_end,
    org_id,
    fuel_id,
    fuel_cost_per_unit,
    emissions_grams_per_unit
  FROM
    (
      SELECT
        org_id,
        fuel_id,
        fuel_cost_per_unit,
        emissions_grams_per_unit,
        EXPLODE(
          SEQUENCE(
            DATE_TRUNC('hour', interval_start),
            DATE_TRUNC('hour', interval_end - INTERVAL 1 HOUR), -- SEQUENCE() takes inclusive end time.
            INTERVAL 1 hour
          )
        ) AS interval_start
      FROM
        effective_custom_fuel_cost_intervals_with_interval_ends
      WHERE
        interval_start < interval_end
        AND interval_end >= ${start_date}
    )
    WHERE DATE(interval_start) >= ${start_date}
    AND DATE(interval_start) < ${end_date}
),
-- We can now combine with the dynamic fuel cost intervals we calculated before
-- Preferentially choosing the custom configuration
fuel_cost_emissions_hour_intervals AS (
  SELECT
    COALESCE(custom.org_id, dynamic_costs.org_id) AS org_id,
    COALESCE(custom.fuel_id, dynamic_costs.fuel_id) AS fuel_id,
    COALESCE(
      custom.fuel_cost_per_unit,
      dynamic_costs.fuel_cost_per_unit
    ) AS cost_per_unit,
    custom.emissions_grams_per_unit AS emissions_grams_per_unit,
    COALESCE(
      custom.interval_start,
      dynamic_costs.interval_start
    ) AS interval_start,
    COALESCE(custom.interval_end, dynamic_costs.interval_end) AS interval_end
  FROM
    custom_fuel_cost_hour_intervals AS custom
    FULL OUTER JOIN org_dynamic_fuel_cost_hour_intervals AS dynamic_costs
      ON dynamic_costs.org_id = custom.org_id
      AND dynamic_costs.interval_start = custom.interval_start
      AND dynamic_costs.fuel_id = custom.fuel_id
),

-- Map fuel_id to stored unit type.
-- Note that the default unit is per US gallon, so this table only includes other unit types.
fuel_id_to_special_unit_type AS (
  SELECT
    *
  FROM
  VALUES
    ("wh", 4),
    ("kg", 8),
    ("kg", 9),
    ("kg", 11)

  AS tab(unit_type, fuel_id)
),

fuel_cost_emissions_hour_intervals_with_appropriate_unit AS (
  SELECT
    intervals.org_id,
    intervals.fuel_id AS configurable_fuel_id,

    IF (
      special_unit.unit_type IS NOT NULL AND special_unit.unit_type = "wh",
      intervals.cost_per_unit,
      NULL
    ) AS cost_per_kwh,
    IF (
      special_unit.unit_type IS NOT NULL AND special_unit.unit_type = "wh",
      intervals.emissions_grams_per_unit,
      NULL
    ) AS emissions_grams_per_wh,

    IF (
      special_unit.unit_type IS NOT NULL AND special_unit.unit_type = "kg",
      intervals.cost_per_unit,
      NULL
    ) AS cost_per_kg,
    IF (
      special_unit.unit_type IS NOT NULL AND special_unit.unit_type = "kg",
      intervals.emissions_grams_per_unit,
      NULL
    ) AS emissions_grams_per_kg,

    IF (
      special_unit.unit_type IS NULL,
      intervals.cost_per_unit,
      NULL
    ) AS cost_per_us_gal,
    IF (
      special_unit.unit_type IS NULL,
      intervals.emissions_grams_per_unit,
      NULL
    ) AS emissions_grams_per_us_gal,

    intervals.interval_start,
    intervals.interval_end

    FROM
      fuel_cost_emissions_hour_intervals AS intervals
      LEFT JOIN fuel_id_to_special_unit_type AS special_unit
        ON special_unit.fuel_id = intervals.fuel_id
),

-- Finally we add the default emissions where applicable
fuel_cost_emissions_with_default_emissions_hour_intervals AS (
  SELECT
    intervals.org_id,
    intervals.configurable_fuel_id,

    intervals.cost_per_kwh,
    intervals.emissions_grams_per_wh,

    intervals.cost_per_kg,
    COALESCE(intervals.emissions_grams_per_kg, default_emissions.emissions_grams_per_kg) AS emissions_grams_per_kg,

    intervals.cost_per_us_gal,
    COALESCE(intervals.emissions_grams_per_us_gal, default_emissions.emissions_grams_per_gallon) AS emissions_grams_per_us_gal,

    intervals.interval_start,
    intervals.interval_end
  FROM
    fuel_cost_emissions_hour_intervals_with_appropriate_unit AS intervals
    LEFT JOIN default_emissions_per_org AS default_emissions
      ON default_emissions.org_id = intervals.org_id
      AND default_emissions.fuel_id = intervals.configurable_fuel_id
)

SELECT
  DATE(interval_start) AS date,
  org_id,
  configurable_fuel_id,
  cost_per_kwh,
  emissions_grams_per_wh,
  cost_per_kg,
  emissions_grams_per_kg,
  cost_per_us_gal,
  emissions_grams_per_us_gal,
  interval_start,
  interval_end
FROM
  fuel_cost_emissions_with_default_emissions_hour_intervals
