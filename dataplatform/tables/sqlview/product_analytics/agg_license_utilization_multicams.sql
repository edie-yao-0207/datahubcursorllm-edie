WITH active_multicam_licenses_by_sam AS (
  SELECT sam_number, SUM(product_net_qty) AS active_licenses
  FROM edw.silver.fct_license_orders
  WHERE is_contract_active IS TRUE
    AND product_sku = 'LIC-MC-AIM4'
  GROUP BY 1
  HAVING active_licenses > 0
)
, installed_multicams_last_365_days AS (
  SELECT
    do.sam_number
    , COUNT(DISTINCT ldo.device_id) AS installed_devices
  FROM datamodel_core.dim_product_variants dpv
  JOIN datamodel_core.dim_devices dd
    ON dpv.product_id = dd.product_id
    AND dpv.variant_id = dd.variant_id
  JOIN datamodel_core.dim_organizations do
    ON dd.org_id = do.org_id
  JOIN datamodel_core.lifetime_device_online ldo
    ON dd.org_id = ldo.org_id
    AND dd.date = ldo.date
    AND dd.device_id = ldo.device_id -- Count of installed devices (Active in last 365 days - used this to minimize replacement counts. If a customer had installed device 1, and got a replacement device 2 in 365 days, both will be counted here)
  WHERE
    dpv.hw_variant_name = 'HW-MC-AIM4'
    AND dd.date = (SELECT MAX(date) FROM datamodel_core.dim_devices)
    AND do.date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)
    AND do.internal_type_name = 'Customer Org'
    AND do.is_simulated_org IS FALSE
    AND ldo.latest_date >= CAST(DATE_SUB(CURRENT_DATE, 365) AS STRING)
    AND ldo.latest_date <= CAST(CURRENT_DATE AS STRING)
  GROUP BY 1
)
, active_multicams_last_30_days AS (
  SELECT o.sam_number, COUNT(DISTINCT fu.device_id) AS active_multicams
  FROM dataprep_firmware.cm_final_uptime fu
  JOIN datamodel_core.dim_organizations o
    ON o.org_id = fu.org_id
  WHERE fu.date >= DATE_SUB(CURRENT_DATE, 30)
    AND fu.date <= CURRENT_DATE
    AND fu.product_name = 'AIM4'
    AND fu.target_recording_ms IS NOT NULL
    AND o.date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)
    AND o.internal_type_name = 'Customer Org'
    AND o.is_simulated_org IS FALSE
  GROUP BY 1
)
SELECT
  lic.sam_number
  , lic.active_licenses
  , COALESCE(inst.installed_devices, 0) AS installed_devices
  , COALESCE(act.active_multicams, 0) AS active_multicams
FROM active_multicam_licenses_by_sam lic
LEFT JOIN installed_multicams_last_365_days inst
  ON lic.sam_number = inst.sam_number
LEFT JOIN active_multicams_last_30_days act
  ON lic.sam_number = act.sam_number
