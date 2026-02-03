WITH dynamic_fuel_cost_locales_org_id AS (
  -- Find all organizations and their locale.
  SELECT
    id AS org_id,
    locale AS locale_id
  FROM clouddb.organizations
),

-- Find dynamic fuel cost intervals for each org based on their locale.
dynamic_fuel_cost_intervals AS (
  SELECT
    DATE(from_unixtime(updated_at / 1000)) as date,
    org_id,
    fuel_cost,
    dfcloi.locale_id,
    from_unixtime(updated_at / 1000) AS interval_start,
    COALESCE(
      from_unixtime((lead(updated_at) OVER (PARTITION BY org_id ORDER BY updated_at ASC)) / 1000),
      CAST (${end_date} AS TIMESTAMP)
    ) AS interval_end
  FROM
    localedb.locale_fuel_costs AS lfc
  JOIN dynamic_fuel_cost_locales_org_id AS dfcloi
  ON lfc.locale_id = dfcloi.locale_id
  WHERE lfc.configurable_fuel_id = 0
),

-- Expand fuel cost intervals into hour intervals.
dynamic_fuel_cost_hour_intervals AS (
  SELECT
    DATE(interval_start) AS date, -- keep date in sync with each hour interval start
    org_id,
    interval_start,
    fuel_cost
  FROM (
    SELECT
      date,
      org_id,
      EXPLODE(
        SEQUENCE(
          DATE_TRUNC('hour', interval_start),
          DATE_TRUNC('hour', interval_end), -- SEQUENCE() takes inclusive end time.
          INTERVAL 1 hour
        )
      ) AS interval_start,
      fuel_cost
    FROM
      dynamic_fuel_cost_intervals
    WHERE interval_start < interval_end
  )
)

-- Average dynamic fuel cost by hour, if there are multiple entries per hour.
SELECT
  date,
  org_id,
  interval_start AS interval_start,
  interval_start + INTERVAL 1 HOUR AS interval_end,
  AVG(fuel_cost) AS dynamic_fuel_cost
FROM dynamic_fuel_cost_hour_intervals
WHERE date >= ${start_date}
AND date < ${end_date}
GROUP BY org_id, interval_start, date
