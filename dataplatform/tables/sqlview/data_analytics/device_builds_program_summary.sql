SELECT
  t1.date,
  t1.org_id,
  t1.device_id,
  t1.latest_gateway_id,
  t1.active_gateways_on_day,
  t1.latest_build_on_day,
  t1.active_builds_on_day,
  t1.boot_counts_on_day,
  t2.product_program_id,
  t2.product_program_id_type
FROM dataprep.device_builds t1
LEFT JOIN dataprep.device_product_programs t2
  ON t1.latest_gateway_id = t2.latest_gateway_id
  AND t1.device_id = t2.device_id
  AND t1.org_id = t2.org_id
  AND t1.date = t2.date
