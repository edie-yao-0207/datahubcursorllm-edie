-- This view is sourced from the 'uat_netsuite_child_invoices' and has undergone
-- transformation of identifiers and sanitization of personally identifiable information
-- to ensure no collision between the User Acceptance Testing (UAT) and production data.
SELECT
  order_type,
  create_date,
  _fivetran_synced,
  _fivetran_deleted,
  netsuite_data.set_namespace_string(invoice_number) AS invoice_number,
  netsuite_data.set_namespace_bigint(transaction_id) AS transaction_id,
  netsuite_data.set_namespace_string(sam_number) AS sam_number,
  cost_amount,
  shipping_amount,
  tax_amount,
  payment_amount,
  symbol,
  amount_unbilled,
  exchange_rate,
  invoice_status,
  due_date,
  payment_status,
  charge_error_message,
  netsuite_data.set_namespace_string(po_number) AS po_number,
  netsuite_data.set_namespace_bigint(netsuite_internal_id) AS netsuite_internal_id,
  netsuite_data.set_namespace_string(order_number) AS order_number,
  netsuite_data.replace_and_hash(billing_addressee, 'Samsara') AS billing_addressee,
  netsuite_data.replace_and_hash(billing_attention, 'Samsara') AS billing_attention,
  netsuite_data.replace_and_hash(bill_address_line_1, '1 De Haro St.') AS bill_address_line_1,
  netsuite_data.replace_and_hash(bill_address_line_2, 'Attn:') AS bill_address_line_2,
  netsuite_data.replace_and_hash(bill_city, 'San Francisco') AS bill_city,
  netsuite_data.replace_and_hash(bill_state, 'California') AS bill_state,
  netsuite_data.replace_and_hash(bill_zip, '94107') AS bill_zip,
  netsuite_data.replace_and_hash(bill_country, 'USA') AS bill_country,
  netsuite_data.replace_and_hash(bill_phone_number, '(415) 985-2400') AS bill_phone_number,
  netsuite_data.replace_and_hash(ship_attention, 'Samsara') AS ship_attention,
  netsuite_data.replace_and_hash(shipping_addressee, 'Samsara') AS shipping_addressee,
  netsuite_data.replace_and_hash(ship_address_line_1, '1 De Haro St.') AS ship_address_line_1,
  netsuite_data.replace_and_hash(ship_address_line_2, 'Attn:') AS ship_address_line_2,
  netsuite_data.replace_and_hash(ship_city, 'San Francisco') AS ship_city,
  netsuite_data.replace_and_hash(ship_state, 'California') AS ship_state,
  netsuite_data.replace_and_hash(ship_zip, '94107') AS ship_zip,
  netsuite_data.replace_and_hash(ship_country, 'USA') AS ship_country,
  netsuite_data.replace_and_hash(ship_phone_number, '(415) 985-2400') AS ship_phone_number,
  netsuite_data.set_namespace_bigint(consolidated_invoice_id) AS consolidated_invoice_id,
  total_amount
FROM
  netsuite_data.uat_netsuite_child_invoices
WHERE
  sam_number IN (SELECT sam_number FROM finopsdb.uat_sam_number_allowlist)
