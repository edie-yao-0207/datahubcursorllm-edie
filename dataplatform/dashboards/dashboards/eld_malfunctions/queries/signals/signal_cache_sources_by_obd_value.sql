-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Signal Sources
-- View: Signal Cache Sources by OBD Value
-- =============================================================================
-- Visualization: Table
-- Title: "Signal Cache Sources by OBD Value"
-- Description: Flattened view of signal cache sources with one row per OBD value source per device per day. Shows all sources, statistics, and timing for each OBD signal. RECOMMENDED: Set make, model, year, cable_name, product_name, and obd_value_name parameters to improve query performance by filtering early.
-- =============================================================================

WITH
-- Get latest device dimensions for filtering
latest_device_dimensions AS (
    SELECT
        org_id,
        device_id,
        cable_id,
        cable_name,
        product_id,
        product_name
    FROM product_analytics.dim_device_dimensions
    WHERE date = (SELECT MAX(date) FROM product_analytics.dim_device_dimensions)
),

-- Get latest vehicle properties for filtering
latest_vehicle_properties AS (
    SELECT
        org_id,
        device_id,
        make,
        model,
        year,
        powertrain,
        fuel_group
    FROM product_analytics_staging.dim_device_vehicle_properties
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
),

-- Base signal cache data with dimensions
signal_cache_with_dimensions AS (
    SELECT
        sc.date,
        sc.org_id,
        sc.device_id,
        sc.obd_value_to_sources_map,
        sc.total_obd_values,
        sc.total_unique_sources,
        dd.cable_id,
        dd.cable_name,
        dd.product_id,
        dd.product_name,
        vp.make,
        vp.model,
        vp.year,
        vp.powertrain,
        vp.fuel_group
    FROM datamodel_dev.agg_signal_cache_sources_by_obd_value sc
    LEFT JOIN latest_device_dimensions dd
        ON sc.org_id = dd.org_id
        AND sc.device_id = dd.device_id
    LEFT JOIN latest_vehicle_properties vp
        ON sc.org_id = vp.org_id
        AND sc.device_id = vp.device_id
    WHERE sc.date BETWEEN :date.min AND :date.max
        AND sc.obd_value_to_sources_map IS NOT NULL
        -- Required device_id filter
        AND sc.device_id = CAST(:device_id AS BIGINT)
        -- Optional filters for performance
        AND (:make = '' OR vp.make = :make)
        AND (:model = '' OR vp.model = :model)
        AND (:year = '' OR vp.year = CAST(:year AS BIGINT))
        AND (:cable_name = '' OR dd.cable_name = :cable_name)
        AND (:product_name = '' OR dd.product_name = :product_name)
),

-- Explode map to get individual OBD values with their data
map_exploded AS (
    SELECT
        scd.date,
        scd.org_id,
        scd.device_id,
        obd_value,
        scd.obd_value_to_sources_map[obd_value] AS signal_data,
        scd.total_obd_values,
        scd.total_unique_sources,
        scd.cable_id,
        scd.cable_name,
        scd.product_id,
        scd.product_name,
        scd.make,
        scd.model,
        scd.year,
        scd.powertrain,
        scd.fuel_group
    FROM signal_cache_with_dimensions scd
    LATERAL VIEW explode(MAP_KEYS(scd.obd_value_to_sources_map)) AS obd_value
),

-- Join with definition table to get translated names
map_with_translations AS (
    SELECT
        me.*,
        COALESCE(obd_def.name, CAST(me.obd_value AS STRING)) AS obd_value_name
    FROM map_exploded me
    LEFT JOIN definitions.obd_values AS obd_def
        ON me.obd_value = obd_def.id
    WHERE (SIZE(:obd_value_name) = 0 OR :obd_value_name IS NULL OR array_contains(:obd_value_name, COALESCE(obd_def.name, CAST(me.obd_value AS STRING))))
),

-- Explode sources array to show individual sources
sources_exploded AS (
    SELECT
        mwt.date,
        mwt.org_id,
        mwt.device_id,
        mwt.obd_value,
        mwt.obd_value_name,
        mwt.cable_name,
        mwt.product_name,
        mwt.make,
        mwt.model,
        mwt.year,
        mwt.powertrain,
        mwt.fuel_group,
        source.bus_id,
        source.ecu_id,
        source.request_id,
        source.data_id,
        -- Per-source statistics
        source.source_stats.mean_value AS source_mean_value,
        source.source_stats.median_value AS source_median_value,
        source.source_stats.min_value AS source_min_value,
        source.source_stats.max_value AS source_max_value,
        source.source_stats.p25_value AS source_p25_value,
        source.source_stats.p75_value AS source_p75_value,
        -- Aggregated statistics across all sources
        mwt.signal_data.value_stats.min_value,
        mwt.signal_data.value_stats.max_value,
        mwt.signal_data.value_stats.p25_value,
        mwt.signal_data.value_stats.p50_value,
        mwt.signal_data.value_stats.p75_value,
        mwt.signal_data.value_stats.mean_value,
        mwt.signal_data.value_stats.value_count,
        mwt.signal_data.timing.first_seen_time,
        mwt.signal_data.timing.last_seen_time,
        mwt.signal_data.timing.observation_count,
        SIZE(mwt.signal_data.sources) AS source_count
    FROM map_with_translations mwt
    LATERAL VIEW explode(mwt.signal_data.sources) AS source
    WHERE mwt.signal_data IS NOT NULL
        AND mwt.signal_data.sources IS NOT NULL
        AND source.source_stats IS NOT NULL
)

SELECT
    CAST(date AS DATE) AS date,
    org_id,
    device_id,
    obd_value_name,
    bus_def.name AS bus_name,
    ecu_id,
    request_id,
    data_id,
    cable_name,
    product_name,
    make,
    model,
    year,
    pt_def.name AS powertrain_name,
    fg_def.name AS fuel_group_name,
    -- Per-source statistics
    source_mean_value,
    source_median_value,
    source_min_value,
    source_max_value,
    source_p25_value,
    source_p75_value,
    -- Aggregated statistics across all sources
    min_value,
    max_value,
    p25_value,
    p50_value,
    p75_value,
    mean_value,
    value_count,
    first_seen_time,
    last_seen_time,
    observation_count,
    source_count
FROM sources_exploded
LEFT JOIN definitions.properties_fuel_powertrain AS pt_def
    ON powertrain = pt_def.id
LEFT JOIN definitions.properties_fuel_fuelgroup AS fg_def
    ON fuel_group = fg_def.id
LEFT JOIN definitions.vehicle_diagnostic_bus AS bus_def
    ON bus_id = bus_def.id
ORDER BY date DESC, obd_value_name, device_id, bus_def.name, ecu_id, request_id, data_id
