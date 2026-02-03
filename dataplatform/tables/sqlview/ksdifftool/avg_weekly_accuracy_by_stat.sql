SELECT
  table_name,
  avg(weekly_accuracy) as avg_weekly_accuracy,
  coalesce(stddev(weekly_accuracy), 0) as std_deviation_across_weeks, -- stddev is NULL when there's only 1 row in the group (i.e we've only collected 1 week of data)
  min(start_date) as start_date,
  max(end_date) as end_date,
  sum(weekly_total_count) as total_count
FROM ksdifftool.weekly_accuracy_by_stat
WHERE start_date >= '2021-10-25' -- We didn't track received_delta_seconds until 10/25, so everything appears 100% "wrong" before then
GROUP BY 1
