-- Calculate difference between current and previous cumulative GPS distance.
SELECT
  DATE(date) AS date,
  org_id,
  object_id,
  time,
  value.double_value - (
    LAG(value.double_value) OVER next_value
  ) AS distance_traveled_m
FROM kinesisstats.osdderivedgpsdistance
WHERE DATE(date) >= DATE_SUB(${start_date}, 1)
AND DATE(date) < ${end_date}
AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
AND object_id IS NOT NULL
WINDOW next_value AS (PARTITION by org_id, object_id ORDER BY time ASC)
