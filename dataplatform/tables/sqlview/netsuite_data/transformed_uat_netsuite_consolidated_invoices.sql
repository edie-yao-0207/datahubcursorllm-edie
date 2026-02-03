-- This view is sourced from the 'uat_netsuite_consolidated_invoices' and has undergone
-- transformation of identifiers and sanitization of personally identifiable information
-- to ensure no collision between the User Acceptance Testing (UAT) and production data.
SELECT
  netsuite_data.set_namespace_string(consolidated_invoice_num) AS consolidated_invoice_num,
  netsuite_data.set_namespace_bigint(consolidated_invoice_id) AS consolidated_invoice_id,
  amount_remaining,
  total_amount,
  due_date,
  create_date,
  _fivetran_synced,
  _fivetran_deleted,
  netsuite_data.set_namespace_string(sam_number) AS sam_number,
  invoice_status,
  currency_symbol,
  payment_status,
  charge_error_message
FROM
  netsuite_data.uat_netsuite_consolidated_invoices
WHERE
  sam_number IN (SELECT sam_number FROM finopsdb.uat_sam_number_allowlist)
