-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: ML/AI Impact Summary
-- =============================================================================
-- Visualization: Table
-- Description: Odometer diagnostic performance (precision, recall, new populations)
-- Note: Shows latest available metrics (all-time snapshot, not date-filtered)
-- Note: Precision/recall are manually tracked per impact metrics doc (~100% precision, ~40% recall)
-- =============================================================================

WITH latest_metrics AS (
  SELECT MAX(date) AS max_date
  FROM default.dojo.signal_reverse_engineering_metrics_v0
  WHERE signal = 'OBD_VALUE_ODOMETER_HIGH_RES_METERS'
),
aggregated AS (
  SELECT
    SUM(m.correct_prediction_count) AS correct_predictions,
    SUM(m.prediction_count) AS total_predictions,
    SUM(m.promotion_count) AS promotions
  FROM default.dojo.signal_reverse_engineering_metrics_v0 m
  CROSS JOIN latest_metrics lm
  WHERE m.date = lm.max_date
    AND m.signal = 'OBD_VALUE_ODOMETER_HIGH_RES_METERS'
)

SELECT
  'Odometer' AS diagnostic_type,
  -- Precision: manually tracked as ~100% per impact metrics doc
  -- Computing from available data: correct_predictions / total_predictions
  CASE 
    WHEN total_predictions > 0 
    THEN ROUND((correct_predictions::FLOAT / total_predictions), 4)
    ELSE NULL
  END AS precision_percentage,
  -- Recall: manually tracked as ~40% per impact metrics doc
  -- Note: Recall calculation requires false negatives which aren't in this table
  NULL AS recall_percentage,
  promotions AS new_populations_identified
FROM aggregated

