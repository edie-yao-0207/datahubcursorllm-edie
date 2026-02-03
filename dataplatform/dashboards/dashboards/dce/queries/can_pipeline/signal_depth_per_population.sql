-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Signal Depth per Population
-- =============================================================================
-- Visualization: Table/Counter
-- Description: Average/median signals per population
-- =============================================================================

WITH signal_counts_per_population AS (
  SELECT 
    mmyef_id,
    COUNT(DISTINCT signal_catalog_id) AS signal_count
  FROM product_analytics_staging.dim_combined_signal_definitions
  WHERE mmyef_id IS NOT NULL
  GROUP BY mmyef_id
)

SELECT
  COUNT(*) AS populations_with_signals,
  ROUND(AVG(signal_count), 1) AS avg_signals_per_population,
  PERCENTILE(signal_count, 0.5) AS median_signals_per_population,
  PERCENTILE(signal_count, 0.25) AS p25_signals,
  PERCENTILE(signal_count, 0.75) AS p75_signals,
  MIN(signal_count) AS min_signals,
  MAX(signal_count) AS max_signals
FROM signal_counts_per_population

