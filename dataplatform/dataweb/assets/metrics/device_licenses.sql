SELECT
  CAST(dlg._run_dt AS STRING) date,
  sam_map.sam_number,
  oc.internal_type,
  dlg.product_sku AS sku,
  xref.product_family,
  xref.sub_product_line,
  xref.device_type,
  MAX(dlg.net_quantity) AS quantity
FROM edw.silver.fct_license_orders_daily_snapshot dlg
JOIN edw.silver.license_hw_sku_xref xref
  ON dlg.product_sku = xref.license_sku
JOIN product_analytics_staging.stg_organization_categories_global oc
  ON dlg.sam_number = COALESCE(oc.salesforce_sam_number, oc.sam_number)
  AND oc.date = CAST(dlg._run_dt AS STRING)
LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
  ON oc.sam_number = sam_map.sam_number
WHERE
  dlg.net_quantity > 0
  AND xref.is_core_license = TRUE
GROUP BY ALL
