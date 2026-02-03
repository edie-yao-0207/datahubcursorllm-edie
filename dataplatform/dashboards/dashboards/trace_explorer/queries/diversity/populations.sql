-- =============================================================================
-- Dashboard: VDP Trace Explorer
-- Tab: Trace Diversity
-- View: Populations
-- =============================================================================
-- Visualization: Table
-- Description: Population coverage by MMYEF
-- =============================================================================

WITH overall_populations AS (
    SELECT mmyef_id, make, model, year, engine_model, powertrain, fuel_group, trim, COUNT(*) AS count
    FROM product_analytics_staging.dim_device_vehicle_properties
    WHERE date BETWEEN :date.min AND :date.max
    GROUP BY ALL
),

traces AS (
    SELECT mmyef_id, COUNT(DISTINCT trace_uuid) AS count_traces
    FROM product_analytics_staging.dim_trace_characteristics
    JOIN product_analytics_staging.dim_device_vehicle_properties USING (date, org_id, device_id)
    WHERE date BETWEEN :date.min AND :date.max
    GROUP BY ALL
)

SELECT overall_populations.*, traces.count_traces, COALESCE(traces.count_traces > 0, False) AS has_traces
FROM overall_populations
LEFT JOIN traces USING (mmyef_id)

