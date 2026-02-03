-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Data Health
-- View: Daily Valid Event Count by Type
-- =============================================================================
-- Visualization: Line Chart
-- Title: "Daily Valid Event Count by Type"
-- Description: Daily count of valid events per type
-- =============================================================================

SELECT
    event_type,
    date AS event_date,
    SUM(valid_count) AS valid_rows
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
GROUP BY event_type, date
ORDER BY date, event_type
