WITH stats_with_prev_value (
  SELECT
    DATE(date) as date,
    org_id,
    object_id,
    time,
    int_value,
    LAG(int_value) OVER (PARTITION BY org_id, object_id ORDER BY time ASC) AS prev_int_value
  FROM engine_state.corrected_stats
  WHERE date >= DATE_SUB(${start_date}, 30)
    AND date < ${end_date}
)

-- Skip engine state points with no state changes.
SELECT *
FROM stats_with_prev_value
WHERE (prev_int_value IS NULL OR int_value != prev_int_value)
  AND date >= ${start_date}
  AND date < ${end_date}
