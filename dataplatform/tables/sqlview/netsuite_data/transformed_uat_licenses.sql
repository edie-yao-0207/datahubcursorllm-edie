-- This view is sourced from the 'uat_licenses' and has undergone
-- transformation of identifiers and sanitization of personally identifiable information
-- to ensure no collision between the User Acceptance Testing (UAT) and production data.
SELECT
  sku,
  netsuite_data.set_namespace_string(sam_number) AS sam_number,
  netsuite_data.set_namespace_bigint(netsuite_transaction_id) AS netsuite_transaction_id,
  netsuite_transaction_line_id,
  netsuite_data.set_namespace_string(return_number) AS return_number,
  netsuite_data.set_namespace_string(order_number) AS order_number,
  quantity,
  transaction_type,
  _fivetran_synced,
  _fivetran_deleted,
  status,
  order_created_at,
  return_type_id,
  return_subtype_id,
  netsuite_data.set_namespace_bigint(created_from_id) AS created_from_id,
  created_from_status,
  start_date,
  end_date
FROM
  netsuite_data.uat_licenses
WHERE
  sam_number IN (SELECT sam_number FROM finopsdb.uat_sam_number_allowlist)
