-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Comparison
-- View: Valid vs Invalid Rows by Event Type
-- =============================================================================
-- Visualization: Stacked Bar Chart
-- Title: "Valid vs Invalid Rows by Event Type"
-- Description: Stacked bar showing valid and invalid rows for each event type
-- =============================================================================

SELECT
    event_type,
    SUM(valid_count) AS valid_rows,
    SUM(invalid_count) AS invalid_rows
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
GROUP BY event_type
ORDER BY (SUM(valid_count) + SUM(invalid_count)) DESC
