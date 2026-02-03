-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Product/Cable Combination Metrics
-- =============================================================================
-- Visualization: Counter
-- Description: Unique product/cable combinations and product/cable/mmyef combinations
-- Note: Uses latest available date for device dimensions, all-time snapshot
-- =============================================================================

WITH latest_device_dims AS (
  SELECT MAX(date) AS max_date
  FROM product_analytics.dim_device_dimensions
),
product_cable_combos AS (
  SELECT COUNT(DISTINCT CONCAT(CAST(product_id AS STRING), '_', CAST(cable_id AS STRING))) AS unique_product_cable_combinations
  FROM product_analytics.dim_device_dimensions
  WHERE date = (SELECT max_date FROM latest_device_dims)
    AND product_id IS NOT NULL
    AND cable_id IS NOT NULL
),
product_cable_mmyef_combos AS (
  SELECT COUNT(DISTINCT CONCAT(
    CAST(dd.product_id AS STRING), '_',
    CAST(dd.cable_id AS STRING), '_',
    CAST(COALESCE(dvp.mmyef_id, -1) AS STRING)
  )) AS unique_product_cable_mmyef_combinations
  FROM product_analytics.dim_device_dimensions dd
  LEFT JOIN product_analytics_staging.dim_device_vehicle_properties dvp
    ON dd.date = dvp.date
    AND dd.org_id = dvp.org_id
    AND dd.device_id = dvp.device_id
  WHERE dd.date = (SELECT max_date FROM latest_device_dims)
    AND dd.product_id IS NOT NULL
    AND dd.cable_id IS NOT NULL
)

SELECT
  pcc.unique_product_cable_combinations,
  pcmc.unique_product_cable_mmyef_combinations
FROM product_cable_combos pcc
CROSS JOIN product_cable_mmyef_combos pcmc

