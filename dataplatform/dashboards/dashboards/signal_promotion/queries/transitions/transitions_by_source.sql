-- Transition volume by data source (30-day)
WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_transition_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'data_source'
)

SELECT
  ds.name AS data_source_name,
  m.monthly.transition_count AS transitions_30d,
  -- Daily unique counts (rolling window unique counts not available)
  m.daily.unique_promotions AS unique_promotions,
  m.daily.unique_signals AS unique_signals,
  m.daily.unique_populations AS unique_populations
FROM product_analytics_staging.agg_signal_promotion_transition_metrics m
LEFT JOIN definitions.promotion_data_source_to_name ds ON ds.id = m.data_source
WHERE m.date = (SELECT max_date FROM latest_date)
  AND m.grouping_columns = 'data_source'
  AND m.data_source IS NOT NULL
ORDER BY m.monthly.transition_count DESC
