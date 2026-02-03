-- Rollback analysis - transitions TO stage=1 (FAILED)
WITH latest_date AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics_staging.agg_signal_promotion_transition_metrics
  WHERE date BETWEEN :date.min AND :date.max
    AND grouping_columns = 'from_stage.to_stage'
)

SELECT
  COALESCE(fs.name, 'NULL') AS from_stage,
  m.monthly.transition_count AS rollbacks_30d,
  -- Daily unique counts (rolling window unique counts not available)
  m.daily.unique_promotions AS unique_promotions,
  m.daily.unique_signals AS unique_signals,
  CASE 
    WHEN m.from_stage = 4 THEN 'Beta Rollback'
    WHEN m.from_stage = 5 THEN 'GA Rollback'
    WHEN m.from_stage = 3 THEN 'Alpha Rollback'
    WHEN m.from_stage = 2 THEN 'Pre-Alpha Rollback'
    ELSE 'Other'
  END AS rollback_type
FROM product_analytics_staging.agg_signal_promotion_transition_metrics m
LEFT JOIN definitions.promotion_stage fs ON fs.id = m.from_stage
WHERE m.date = (SELECT max_date FROM latest_date)
  AND m.grouping_columns = 'from_stage.to_stage'
  AND m.to_stage = 1  -- FAILED stage
  AND m.from_stage IS NOT NULL  -- Exclude initial failures
ORDER BY m.monthly.transition_count DESC
