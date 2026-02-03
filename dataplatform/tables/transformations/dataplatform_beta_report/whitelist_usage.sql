WITH annotated_entries AS (
  SELECT
    date,
    org_id,
    object_id AS device_id,
    DATE_TRUNC('hour', FROM_UNIXTIME(value.time / 1000)) AS interval_start,
    value.int_value AS usage
  FROM kinesisstats.osdwifiapwhitelistbytes
  WHERE date >= ${start_date}
  AND date < ${end_date}
  -- Filter to hardies (US)
  -- And Dorhn (US) and Brent Redmont (US)
  -- And Gundon and M Group Services (EU)
  -- We chose these orgs by looking at which orgs consistently have data.
  AND org_id IN (728, 874, 47450, 562949953427431, 562949953422075)
)

-- Hourly rows for whitelist data usage
SELECT
    date,
    org_id,
    device_id,
    interval_start,
    SUM(usage) AS whitelist_usage_bytes
FROM annotated_entries
GROUP BY date, org_id, device_id, interval_start
