-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: ML Model Metrics
-- View: Prediction Accuracy Trend
-- =============================================================================
-- Visualization: Line Chart
-- Description: Number of correctly predicted and promoted MMYEF over time
-- Notes:
--   1. Many predictions aren't evaluated yet, i.e., prediction_count - correct_prediction_count
--      does not mean incorrect predictions!
--   2. Many correct predictions are not (yet) promoted because we already have a request-based decoding.
-- =============================================================================

SELECT
  date,
  signal,
  SUM(promotion_count) AS promotion_count,
  SUM(correct_prediction_count) AS correct_prediction_count,
  SUM(prediction_count) AS prediction_count
FROM default.dojo.signal_reverse_engineering_metrics_v0
WHERE date BETWEEN :date.min AND :date.max
GROUP BY date, signal
ORDER BY date

