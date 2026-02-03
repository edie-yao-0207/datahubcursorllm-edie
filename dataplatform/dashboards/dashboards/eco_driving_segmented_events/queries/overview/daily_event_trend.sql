-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Overview
-- View: Daily Event Count by Type
-- =============================================================================
-- Visualization: Line Chart
-- Title: "Daily Event Count by Type"
-- Description: Daily trend of event counts by type
-- =============================================================================

SELECT 
    event_type, 
    date AS event_date, 
    SUM(event_count) AS event_count
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
GROUP BY event_type, date
ORDER BY date, event_type
