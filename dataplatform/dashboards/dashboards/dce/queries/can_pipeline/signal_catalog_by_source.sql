-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Signal Catalog by Data Source
-- =============================================================================
-- Visualization: Table
-- Description: Breakdown by data source (J1939, Passenger, SPS)
-- =============================================================================

SELECT
  CASE 
    WHEN data_source_id = 1 THEN 'J1939 Standard'
    WHEN data_source_id = 2 THEN 'Other'
    WHEN data_source_id = 3 THEN 'Passenger Signals'
    WHEN data_source_id = 4 THEN 'SPS Promotions'
    WHEN data_source_id = 5 THEN 'AI/ML'
    WHEN data_source_id = 6 THEN 'Promotion Gap'
    ELSE 'Unknown'
  END AS data_source,
  COUNT(DISTINCT signal_catalog_id) AS signal_count,
  COUNT(DISTINCT mmyef_id) AS populations_covered,
  COUNT(DISTINCT obd_value) AS unique_obd_values
FROM product_analytics_staging.dim_combined_signal_definitions
GROUP BY data_source_id
ORDER BY signal_count DESC

