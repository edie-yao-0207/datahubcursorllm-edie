-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Data Health
-- View: Data Health Summary by Event Type
-- =============================================================================
-- Visualization: Table
-- Title: "Data Health Summary by Event Type"
-- Description: Summary of total rows, valid rows, invalid rows, and percentages per event type
-- =============================================================================

SELECT
    event_type,
    SUM(event_count) AS total_rows,
    SUM(valid_count) AS valid_rows,
    SUM(invalid_count) AS invalid_rows,
    100.0 * SUM(valid_count) / SUM(event_count) AS percent_valid,
    100.0 * SUM(invalid_count) / SUM(event_count) AS percent_invalid
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
GROUP BY event_type
ORDER BY total_rows DESC
