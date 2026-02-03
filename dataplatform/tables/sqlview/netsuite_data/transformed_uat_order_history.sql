-- This view is sourced from the 'uat_exchange_history' and has undergone
-- transformation of identifiers and sanitization of personally identifiable information
-- to ensure no collision between the User Acceptance Testing (UAT) and production data.
SELECT
  netsuite_data.set_namespace_bigint(sales_order_transaction_id) AS sales_order_transaction_id,
  netsuite_data.set_namespace_bigint(item_fulfillment_transaction_id) AS item_fulfillment_transaction_id,
  item_fulfillment_transaction_line_id,
  netsuite_data.set_namespace_string(order_number) AS order_number,
  netsuite_data.set_namespace_string(sam_number) AS sam_number,
  is_transfer_order,
  item_sku,
  item_name,
  item_quantity,
  serial_number,
  sales_order_status,
  item_fulfillment_status,
  netsuite_data.set_namespace_string(tracking_number) AS tracking_number,
  netsuite_data.replace_and_hash(shipping_contact, 'Samsara') AS shipping_contact,
  netsuite_data.replace_and_hash(shipping_contact_email_address, 'sammy@samsara.com') AS shipping_contact_email_address,
  delivery_carrier,
  netsuite_data.replace_and_hash(purchased_by_email, 'sammy@samsara.com') AS purchased_by_email,
  order_created_at,
  shipped_at,
  netsuite_data.replace_and_hash(ship_city, 'San Francisco') AS ship_city,
  netsuite_data.replace_and_hash(ship_country, 'USA') AS ship_country,
  netsuite_data.replace_and_hash(ship_state, 'California') AS ship_state,
  netsuite_data.replace_and_hash(ship_zip, '94107') AS ship_zip,
  netsuite_data.replace_and_hash(ship_address_line_1, '1 De Haro St.') AS ship_address_line_1,
  netsuite_data.replace_and_hash(ship_address_line_2, 'Attn:') AS ship_address_line_2,
  netsuite_data.replace_and_hash(ship_address_line_3, 'Attn:') AS ship_address_line_3,
  netsuite_data.replace_and_hash(bill_city, 'San Francisco') AS bill_city,
  netsuite_data.replace_and_hash(bill_country, 'USA') AS bill_country,
  netsuite_data.replace_and_hash(bill_state, 'California') AS bill_state,
  netsuite_data.replace_and_hash(bill_zip, '94107') AS bill_zip,
  netsuite_data.replace_and_hash(bill_address_line_1, '1 De Haro St.') AS bill_address_line_1,
  netsuite_data.replace_and_hash(bill_address_line_2, 'Attn:') AS bill_address_line_2,
  netsuite_data.replace_and_hash(bill_address_line_3, 'Attn:') AS bill_address_line_3,
  _fivetran_synced,
  _fivetran_deleted
FROM
  netsuite_data.uat_order_history
WHERE
  sam_number IN (SELECT sam_number FROM finopsdb.uat_sam_number_allowlist)
  AND
  serial_number IN (SELECT device_serial FROM finopsdb.uat_device_allowlist)
