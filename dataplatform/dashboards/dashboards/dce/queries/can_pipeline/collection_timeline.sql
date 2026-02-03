-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Collection Timeline
-- =============================================================================
-- Visualization: Counter/Text
-- Description: Collection period, total traces, average per day
-- =============================================================================

SELECT
  COUNT(DISTINCT trace_uuid) AS total_traces,
  MIN(date) AS first_collection_date,
  MAX(date) AS last_collection_date,
  DATEDIFF(MAX(date), MIN(date)) AS days_of_collection,
  ROUND(COUNT(DISTINCT trace_uuid)::FLOAT / NULLIF(DATEDIFF(MAX(date), MIN(date)), 0), 1) AS avg_traces_per_day
FROM product_analytics_staging.fct_can_trace_status
WHERE date BETWEEN :date.min AND :date.max
  AND is_available = TRUE

