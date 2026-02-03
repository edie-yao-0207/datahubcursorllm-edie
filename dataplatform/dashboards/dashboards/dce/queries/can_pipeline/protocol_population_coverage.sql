-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Protocol Population Coverage
-- =============================================================================
-- Visualization: Table
-- Description: Populations by protocol (all-time historical data)
-- Note: This shows all-time totals to match impact metrics document
-- =============================================================================

SELECT
  CASE 
    WHEN application_id = 0 THEN 'NONE/Regular'
    WHEN application_id = 1 THEN 'UDS'
    WHEN application_id = 3 THEN 'J1939'
    ELSE 'Unknown'
  END AS protocol,
  COUNT(DISTINCT mmyef_id) AS populations
FROM product_analytics_staging.fct_can_trace_recompiled
WHERE mmyef_id IS NOT NULL
GROUP BY application_id
ORDER BY populations DESC

