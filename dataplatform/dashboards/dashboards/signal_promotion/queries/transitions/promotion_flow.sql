-- Promotion flow - forward progressions through stages (30-day)
WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_transition_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'from_stage.to_stage'
)

SELECT
  COALESCE(fs.name, 'NULL (Creation)') AS from_stage,
  ts.name AS to_stage,
  m.monthly.transition_count AS promotions_30d,
  -- Daily unique promotions (rolling window unique counts not available)
  m.daily.unique_promotions AS unique_promotions
FROM product_analytics_staging.agg_signal_promotion_transition_metrics m
LEFT JOIN definitions.promotion_stage fs ON fs.id = m.from_stage
LEFT JOIN definitions.promotion_stage ts ON ts.id = m.to_stage
WHERE m.date = (SELECT max_date FROM latest_date)
  AND m.grouping_columns = 'from_stage.to_stage'
  -- Only forward progressions (higher stage number)
  AND (m.from_stage IS NULL OR m.to_stage > m.from_stage)
  AND m.to_stage != 1  -- Exclude failures
ORDER BY m.from_stage NULLS FIRST, m.to_stage
