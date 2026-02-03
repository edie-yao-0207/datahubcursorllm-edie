-- =============================================================================
-- Dashboard: Eco Driving Segmented Events Dashboard
-- Dataset: Event Types Lookup
-- =============================================================================
-- Returns the list of eco driving event types for filter dropdown
-- =============================================================================

SELECT DISTINCT event_type
FROM product_analytics_staging.agg_eco_driving_segmented_events
WHERE date >= DATE_ADD(CURRENT_DATE(), -30)
ORDER BY event_type
