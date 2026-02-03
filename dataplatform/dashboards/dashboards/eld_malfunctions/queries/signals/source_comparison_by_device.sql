-- =============================================================================
-- Dashboard: ELD Malfunctions Dashboard
-- Tab: Source Comparison
-- View: Source Comparison by Device
-- =============================================================================
-- Visualization: Table
-- Title: "Source Comparison by Device"
-- Description: Device-level view showing source comparison metrics for each OBD signal. Shows how different sources agree or differ for the same signal on individual devices. RECOMMENDED: Set make, model, year, cable_name, product_name, obd_value_name, and device_id parameters to improve query performance by filtering early.
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

-- Explode the map to get individual OBD values with their comparison metrics
source_comparison_exploded AS (
    SELECT
        sc.date,
        sc.org_id,
        sc.device_id,
        obd_value_key AS obd_value,
        comparison_data.source_count,
        comparison_data.cv,
        comparison_data.range_ratio,
        comparison_data.iqr,
        comparison_data.agreement_score,
        comparison_data.max_divergence_pct,
        comparison_data.mean_value,
        comparison_data.median_value,
        comparison_data.min_value,
        comparison_data.max_value
    FROM datamodel_dev.agg_signal_source_comparison_daily sc
    LATERAL VIEW explode(obd_value_to_comparison_map) AS obd_value_key, comparison_data
    WHERE sc.date BETWEEN :date.min AND :date.max
        -- Required device_id filter
        AND sc.device_id = CAST(:device_id AS BIGINT)
        AND comparison_data IS NOT NULL
),

-- Base source comparison data with dimensions
source_comparison_with_dimensions AS (
    SELECT
        sce.date,
        sce.org_id,
        sce.device_id,
        sce.obd_value,
        sce.source_count,
        sce.cv,
        sce.range_ratio,
        sce.iqr,
        sce.agreement_score,
        sce.max_divergence_pct,
        sce.mean_value,
        sce.median_value,
        sce.min_value,
        sce.max_value,
        dd.cable_id,
        dd.cable_name,
        dd.product_id,
        dd.product_name,
        vp.make,
        vp.model,
        vp.year,
        vp.powertrain,
        vp.fuel_group
    FROM source_comparison_exploded sce
    LEFT JOIN latest_device_dimensions dd
        ON sce.org_id = dd.org_id
        AND sce.device_id = dd.device_id
    LEFT JOIN latest_vehicle_properties vp
        ON sce.org_id = vp.org_id
        AND sce.device_id = vp.device_id
    WHERE -- Optional filters for performance
        (:make = '' OR vp.make = :make)
        AND (:model = '' OR vp.model = :model)
        AND (:year = '' OR vp.year = CAST(:year AS BIGINT))
        AND (:cable_name = '' OR dd.cable_name = :cable_name)
        AND (:product_name = '' OR dd.product_name = :product_name)
),

-- Join with definition tables to get translated names
source_comparison_with_translations AS (
    SELECT
        scd.*,
        COALESCE(obd_def.name, CAST(scd.obd_value AS STRING)) AS obd_value_name
    FROM source_comparison_with_dimensions scd
    LEFT JOIN definitions.obd_values AS obd_def
        ON scd.obd_value = obd_def.id
    WHERE (SIZE(:obd_value_name) = 0 OR :obd_value_name IS NULL OR array_contains(:obd_value_name, COALESCE(obd_def.name, CAST(scd.obd_value AS STRING))))
)

SELECT
    CAST(date AS DATE) AS date,
    org_id,
    device_id,
    obd_value_name,
    source_count,
    cv,
    range_ratio,
    iqr,
    agreement_score,
    max_divergence_pct,
    mean_value,
    median_value,
    min_value,
    max_value,
    cable_name,
    product_name,
    make,
    model,
    year,
    pt_def.name AS powertrain_name,
    fg_def.name AS fuel_group_name
FROM source_comparison_with_translations
LEFT JOIN definitions.properties_fuel_powertrain AS pt_def
    ON powertrain = pt_def.id
LEFT JOIN definitions.properties_fuel_fuelgroup AS fg_def
    ON fuel_group = fg_def.id
ORDER BY date DESC, obd_value_name, device_id

