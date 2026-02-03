-- This view is sourced from the 'uat_netsuite_deletion' and has undergone
-- transformation of identifiers and sanitization of personally identifiable information
-- to ensure no collision between the User Acceptance Testing (UAT) and production data.
SELECT
  netsuite_data.set_namespace_bigint(transaction_id) AS transaction_id,
  netsuite_data.set_namespace_string(sam_number) AS sam_number,
  _fivetran_synced,
  _fivetran_deleted,
  date_deleted
FROM
  netsuite_data.uat_netsuite_deletion
WHERE
  sam_number IN (SELECT sam_number FROM finopsdb.uat_sam_number_allowlist)
