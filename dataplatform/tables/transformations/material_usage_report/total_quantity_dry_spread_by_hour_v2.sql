WITH dry_deltas AS (
  SELECT
    org_id,
    object_id AS device_id,
    date,
    time,
    value.int_value AS dry_spread_delta,
    date_trunc(
      'hour',
      from_unixtime(time / 1000)
    ) as hour_start
  FROM
    kinesisstats.osdsaltspreaderquantityspreaddrymilligrams
  WHERE
    date >= ${start_date}
    AND date < ${end_date}
    AND NOT value.is_databreak
    AND NOT value.is_end
    AND value.int_value > 0
    AND value.int_value < 1000000000
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
      sum(dry_spread_delta) as total_quantity_spread
    FROM
      dry_deltas
    GROUP BY
      date,
      org_id,
      device_id,
      hour_start
  )
WHERE
  total_quantity_spread > 0
