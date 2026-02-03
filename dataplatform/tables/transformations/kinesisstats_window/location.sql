WITH adjacent_values AS (
  SELECT
    *,
    LAG(STRUCT(*)) OVER ascending_time AS previous,
    LEAD(STRUCT(*)) OVER ascending_time AS next
  FROM
    kinesisstats.location AS eg
  WHERE
    DATE(date) >= DATE_SUB(${start_date}, 1)
    AND DATE(date) < ${end_date}
    AND NOT EXISTS (
      SELECT 1 FROM helpers.ignored_org_ids AS i WHERE i.org_id = eg.org_id)
  WINDOW ascending_time AS (PARTITION BY org_id, device_id ORDER BY time ASC)
)

SELECT
  *
FROM
  adjacent_values
WHERE
  DATE(date) >= ${start_date}
