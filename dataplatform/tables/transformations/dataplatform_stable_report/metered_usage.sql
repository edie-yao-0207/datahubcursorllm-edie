WITH annotated_entries AS (
  SELECT
    date,
    org_id,
    object_id AS device_id,
    DATE_TRUNC('hour', FROM_UNIXTIME(value.time / 1000)) AS interval_start,
    value.int_value AS usage
  FROM kinesisstats.osdwifiapbytes
  WHERE date >= ${start_date}
  AND date < ${end_date}
  AND org_id != 0
)

-- Hourly rows for metered data usage
SELECT
    date,
    org_id,
    device_id,
    interval_start,
    SUM(usage) AS metered_usage_bytes
FROM annotated_entries
GROUP BY date, org_id, device_id, interval_start
