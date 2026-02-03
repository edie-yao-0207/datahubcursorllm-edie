-- Calculate difference between current and previous cumulative energy consumption.
SELECT
  DATE(date) AS date,
  org_id,
  object_id,
  time,
  value.int_value - (
    LAG(value.int_value) OVER next_value) AS energy_consumed_uwh
FROM kinesisstats.osdderivedevenergyconsumedmicrowh
WHERE DATE(date) >= DATE_SUB(${start_date}, 1)
AND DATE(date) < ${end_date}
AND org_id NOT IN (SELECT org_id FROM helpers.ignored_org_ids)
WINDOW next_value AS (PARTITION BY org_id, object_id ORDER BY time ASC)
