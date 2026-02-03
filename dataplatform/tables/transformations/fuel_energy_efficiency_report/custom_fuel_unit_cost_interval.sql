-- Turn fuel cost values into intervals.
WITH custom_fuel_cost_intervals AS (
  SELECT
    TO_DATE(from_unixtime(updated_at / 1000)) AS date,
    org_id,
    fuel_cost,
    from_unixtime(updated_at / 1000) AS interval_start,
    COALESCE(
      from_unixtime(
        (LEAD(updated_at) OVER (PARTITION BY org_id ORDER BY updated_at ASC)) / 1000),
      CAST (${end_date} AS TIMESTAMP)
    ) AS interval_end
  FROM
    fueldb_shards.custom_fuel_costs
),

-- Expand fuel cost intervals into hour intervals.
custom_fuel_cost_hour_intervals AS (
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
    custom_fuel_cost_intervals
  WHERE interval_start < interval_end
)

-- Average custom fuel cost by hour, if there are multiple entries per hour.
SELECT
  TO_DATE(interval_start) AS date,
  org_id,
  interval_start AS interval_start,
  interval_start + INTERVAL 1 HOUR AS interval_end,
  AVG(fuel_cost) AS custom_fuel_cost
FROM custom_fuel_cost_hour_intervals
WHERE TO_DATE(interval_start) >= ${start_date}
AND TO_DATE(interval_start) < ${end_date}
GROUP BY org_id, interval_start
