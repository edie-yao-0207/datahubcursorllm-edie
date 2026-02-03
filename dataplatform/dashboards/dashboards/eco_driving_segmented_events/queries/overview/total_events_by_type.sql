-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Tab: Overview
-- View: Total Events (All Types)
-- =============================================================================
-- Visualization: Counter
-- Title: "Total Events (All Types)"
-- Description: Total count of all eco driving segmented events
-- =============================================================================

SELECT SUM(event_count) AS total_events
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date BETWEEN :date.min AND :date.max
