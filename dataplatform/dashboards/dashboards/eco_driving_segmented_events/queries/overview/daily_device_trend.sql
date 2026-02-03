-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Overview
-- View: Daily Unique Devices by Event Type
-- =============================================================================
-- Visualization: Line Chart
-- Title: "Daily Unique Devices by Event Type"
-- Description: Daily trend of unique devices reporting each event type
-- =============================================================================

SELECT 
    event_type, 
    date AS event_date, 
    COUNT(DISTINCT object_id) AS unique_devices
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
GROUP BY event_type, date
ORDER BY date, event_type
