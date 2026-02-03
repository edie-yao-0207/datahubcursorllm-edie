-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Source Comparison
-- View: Population Behavior by MMYEF + Cable + Product
-- =============================================================================
-- Visualization: Table
-- Title: "Population Behavior by MMYEF + Cable + Product"
-- Description: Population-level summary showing how signal sources behave across devices within the same MMYEF + cable + product group. Since cable type and platform influence source availability, this groups devices with similar source capabilities. Shows average source comparison metrics, multi-source prevalence, source diversity, and population consistency. Filter by MMYEF, cable, product, or OBD value to investigate specific populations.
-- =============================================================================

WITH
-- Get latest vehicle properties for MMYEF details
latest_vehicle_properties AS (
    SELECT DISTINCT
        mmyef_id,
        make,
        model,
        year,
        engine_model,
        powertrain,
        fuel_group
    FROM product_analytics_staging.dim_device_vehicle_properties
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.dim_device_vehicle_properties)
        AND mmyef_id IS NOT NULL
),

-- Get latest device dimensions for cable and product names
latest_device_dimensions AS (
    SELECT DISTINCT
        cable_id,
        cable_name,
        product_id,
        product_name
    FROM product_analytics.dim_device_dimensions
    WHERE date = (SELECT MAX(date) FROM product_analytics.dim_device_dimensions)
        AND cable_id IS NOT NULL
        AND product_id IS NOT NULL
),

-- Explode the map to get individual OBD values with their behavior metrics
population_behavior_exploded AS (
    SELECT
        pb.date,
        pb.mmyef_id,
        pb.cable_id,
        pb.product_id,
        obd_value_key AS obd_value,
        behavior_data.device_count,
        behavior_data.avg_cv,
        behavior_data.avg_source_count,
        behavior_data.multi_source_pct,
        behavior_data.population_cv,
        behavior_data.source_diversity_index,
        behavior_data.avg_mean_value,
        behavior_data.avg_median_value,
        behavior_data.min_device_median,
        behavior_data.max_device_median
    FROM datamodel_dev.agg_signal_population_behavior_daily pb
    LATERAL VIEW explode(obd_value_to_behavior_map) AS obd_value_key, behavior_data
    WHERE pb.date BETWEEN :date.min AND :date.max
        AND behavior_data IS NOT NULL
),

-- Join with dimension tables for display and filtering
population_with_dimensions AS (
    SELECT
        pbe.*,
        vp.make,
        vp.model,
        vp.year,
        vp.engine_model,
        vp.powertrain,
        vp.fuel_group,
        dd.cable_name,
        dd.product_name
    FROM population_behavior_exploded pbe
    LEFT JOIN latest_vehicle_properties vp
        ON pbe.mmyef_id = vp.mmyef_id
    LEFT JOIN latest_device_dimensions dd
        ON pbe.cable_id = dd.cable_id
        AND pbe.product_id = dd.product_id
    WHERE (:make = '' OR vp.make = :make)
        AND (:model = '' OR vp.model = :model)
        AND (:year = '' OR vp.year = CAST(:year AS BIGINT))
        AND (:cable_name = '' OR dd.cable_name = :cable_name)
        AND (:product_name = '' OR dd.product_name = :product_name)
),

-- Join with definition tables for translations
population_with_translations AS (
    SELECT
        pwd.*,
        COALESCE(obd_def.name, CAST(pwd.obd_value AS STRING)) AS obd_value_name
    FROM population_with_dimensions pwd
    LEFT JOIN definitions.obd_values AS obd_def
        ON pwd.obd_value = obd_def.id
    WHERE (SIZE(:obd_value_name) = 0 OR :obd_value_name IS NULL OR array_contains(:obd_value_name, COALESCE(obd_def.name, CAST(pwd.obd_value AS STRING))))
)

SELECT
    CAST(date AS DATE) AS date,
    mmyef_id,
    make,
    model,
    year,
    engine_model,
    pt_def.name AS powertrain_name,
    fg_def.name AS fuel_group_name,
    cable_id,
    cable_name,
    product_id,
    product_name,
    obd_value_name,
    device_count,
    avg_cv,
    avg_source_count,
    multi_source_pct,
    population_cv,
    source_diversity_index,
    avg_mean_value,
    avg_median_value,
    min_device_median,
    max_device_median
FROM population_with_translations
LEFT JOIN definitions.properties_fuel_powertrain AS pt_def
    ON powertrain = pt_def.id
LEFT JOIN definitions.properties_fuel_fuelgroup AS fg_def
    ON fuel_group = fg_def.id
ORDER BY date DESC, device_count DESC, obd_value_name

