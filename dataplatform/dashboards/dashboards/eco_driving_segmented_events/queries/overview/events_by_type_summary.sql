-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Overview
-- View: Events by Type
-- =============================================================================
-- Visualization: Bar Chart
-- Title: "Events by Type"
-- Description: Total event count breakdown by eco driving event type
-- =============================================================================

SELECT 
    event_type, 
    SUM(event_count) AS event_count
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
GROUP BY event_type
ORDER BY event_count DESC
