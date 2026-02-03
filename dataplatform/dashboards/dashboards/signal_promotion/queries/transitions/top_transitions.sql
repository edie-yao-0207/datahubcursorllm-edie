-- Top stage transitions by volume (30-day)
WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_transition_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'from_stage.from_status.to_stage.to_status'
)

SELECT
  COALESCE(fs.name, 'NULL (Initial)') AS from_stage,
  COALESCE(fst.name, 'N/A') AS from_status,
  ts.name AS to_stage,
  tst.name AS to_status,
  m.monthly.transition_count AS transitions_30d,
  -- Daily unique counts (rolling window unique counts not available)
  m.daily.unique_promotions AS unique_promotions,
  m.daily.unique_signals AS unique_signals
FROM product_analytics_staging.agg_signal_promotion_transition_metrics m
LEFT JOIN definitions.promotion_stage fs ON fs.id = m.from_stage
LEFT JOIN definitions.promotion_status fst ON fst.id = m.from_status
LEFT JOIN definitions.promotion_stage ts ON ts.id = m.to_stage
LEFT JOIN definitions.promotion_status tst ON tst.id = m.to_status
WHERE m.date = (SELECT max_date FROM latest_date)
  AND m.grouping_columns = 'from_stage.from_status.to_stage.to_status'
ORDER BY m.monthly.transition_count DESC
LIMIT 25
