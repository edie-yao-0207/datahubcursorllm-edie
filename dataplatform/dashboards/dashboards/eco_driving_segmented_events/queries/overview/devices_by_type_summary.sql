-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Overview
-- View: Unique Devices by Type
-- =============================================================================
-- Visualization: Bar Chart
-- Title: "Unique Devices by Type"
-- Description: Unique device count breakdown by eco driving event type
-- =============================================================================

SELECT 
    event_type, 
    COUNT(DISTINCT object_id) AS unique_devices
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
GROUP BY event_type
ORDER BY unique_devices DESC
