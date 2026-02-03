WITH

-- select all columns except _filename and _sort_key since they aren't useful
-- here
odometer AS (
  SELECT
    date,
    stat_type,
    org_id,
    object_type,
    object_id,
    time,
    value
  FROM kinesisstats.osdodometer
),

adjacent_values AS (
  SELECT
    *,
    LAG(STRUCT(*)) OVER ascending_time AS previous,
    LEAD(STRUCT(*)) OVER ascending_time AS next
  FROM
    odometer AS eg
  WHERE
    DATE(date) >= DATE_SUB(${start_date}, 1)
    AND DATE(date) < ${end_date}
    AND NOT EXISTS (
      SELECT 1 FROM helpers.ignored_org_ids AS i WHERE i.org_id = eg.org_id
    )
  WINDOW ascending_time AS (PARTITION BY org_id, object_id ORDER BY time ASC)
)

SELECT
  *
FROM
  adjacent_values
WHERE
  DATE(date) >= ${start_date}
