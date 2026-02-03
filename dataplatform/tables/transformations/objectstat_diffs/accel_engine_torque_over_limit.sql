SELECT
  DATE(date) AS date,
  org_id,
  object_id,
  time,
  (
    value.int_value - LAG(value.int_value) OVER next_value
  ) AS accel_engine_torque_over_limit_ms
FROM kinesisstats.osdderivedaccelenginetorqueoverlimitms
WHERE date >= ${start_date}
AND date < ${end_date}
WINDOW next_value AS (PARTITION BY org_id, object_id ORDER BY time ASC)
