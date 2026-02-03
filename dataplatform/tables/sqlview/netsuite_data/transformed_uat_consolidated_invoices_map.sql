-- This view is sourced from the 'uat_consolidated_invoices_map' and has undergone
-- transformation of identifiers and sanitization of personally identifiable information
-- to ensure no collision between the User Acceptance Testing (UAT) and production data.
SELECT
  netsuite_data.set_namespace_bigint(consolidated_invoice_id) AS consolidated_invoice_id,
  netsuite_data.set_namespace_bigint(transaction_id) AS transaction_id,
  netsuite_data.set_namespace_string(sam_number) AS sam_number
FROM
  netsuite_data.uat_consolidated_invoices_map
WHERE
  sam_number IN (SELECT sam_number FROM finopsdb.uat_sam_number_allowlist)
