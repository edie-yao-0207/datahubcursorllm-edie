-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Overview
-- View: Unique Devices
-- =============================================================================
-- Visualization: Counter
-- Title: "Unique Devices"
-- Description: Total unique devices reporting eco driving events
-- =============================================================================

SELECT COUNT(DISTINCT object_id) AS unique_devices
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
