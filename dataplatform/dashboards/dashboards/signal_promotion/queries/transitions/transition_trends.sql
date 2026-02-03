-- Transition volume over time (overall)
SELECT
  CAST(m.date AS DATE) AS date,
  m.daily.transition_count AS daily_transitions,
  m.weekly.transition_count AS weekly_transitions,
  m.monthly.transition_count AS monthly_transitions
FROM product_analytics_staging.agg_signal_promotion_transition_metrics m
WHERE m.date BETWEEN :date.min AND :date.max
  AND m.grouping_columns = 'overall'
ORDER BY m.date
