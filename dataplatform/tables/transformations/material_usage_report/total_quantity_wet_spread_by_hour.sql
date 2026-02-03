WITH wet_deltas AS (
  SELECT
    org_id,
    object_id AS device_id,
    date,
    time,
    date_trunc(
      'hour',
      from_unixtime(time / 1000)
    ) as hour_start,
    value.int_value AS wet_spread_delta
  FROM
    kinesisstats.osdsaltspreaderquantityspreadwetmilliliters
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
    AND NOT value.is_databreak
    AND NOT value.is_end
)
SELECT
  *
FROM
  (
    SELECT
      date(hour_start) as date,
      org_id,
      device_id,
      hour_start,
      sum(wet_spread_delta) as total_quantity_spread
    FROM
      wet_deltas
    GROUP BY
      date,
      org_id,
      device_id,
      hour_start
  )
WHERE
  total_quantity_spread > 0
