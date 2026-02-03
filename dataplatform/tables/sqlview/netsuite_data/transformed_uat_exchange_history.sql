-- This view is sourced from the 'uat_exchange_history' and has undergone
-- transformation of identifiers and sanitization of personally identifiable information
-- to ensure no collision between the User Acceptance Testing (UAT) and production data.
SELECT
  netsuite_data.set_namespace_bigint(transaction_id) AS transaction_id,
  transaction_type,
  transaction_line_id,
  status,
  netsuite_data.set_namespace_string(return_number) AS return_number,
  netsuite_data.set_namespace_string(order_number) AS order_number,
  netsuite_data.set_namespace_string(sam_number) AS sam_number,
  sku_product_returned,
  name_product_returned,
  sku_product_received,
  name_product_received,
  initial_quantity,
  refunded_quantity,
  received_quantity,
  serial_number,
  array_join(array_intersect(
    split(return_serial_list_returned, '\\s+'),
    (SELECT collect_set(device_serial) FROM finopsdb.uat_device_allowlist)
  ), ' ') AS return_serial_list_returned,
  delivery_carrier,
  netsuite_data.replace_and_hash(shipping_contact, 'Samsara') AS shipping_contact,
  netsuite_data.replace_and_hash(shipping_contact_email_address, 'sammy@samsara.com') AS shipping_contact_email_address,
  netsuite_data.replace_and_hash(requested_by_email, 'sammy@samsara.com') AS requested_by_email,
  exchange_created_at,
  netsuite_data.replace_and_hash(ship_city, 'San Francisco') AS ship_city,
  netsuite_data.replace_and_hash(ship_country, 'USA') AS ship_country,
  netsuite_data.replace_and_hash(ship_state, 'California') AS ship_state,
  netsuite_data.replace_and_hash(ship_zip, '94107') AS ship_zip,
  netsuite_data.replace_and_hash(ship_address_line_1, '1 De Haro St.') AS ship_address_line_1,
  netsuite_data.replace_and_hash(ship_address_line_2, 'Attn:') AS ship_address_line_2,
  netsuite_data.replace_and_hash(ship_address_line_3, 'Attn:') AS ship_address_line_3,
  _fivetran_synced,
  _fivetran_deleted
FROM
  netsuite_data.uat_exchange_history
WHERE
  sam_number IN (SELECT sam_number FROM finopsdb.uat_sam_number_allowlist)
  AND
  (CASE
    WHEN
      serial_number IS NOT NULL THEN serial_number IN (SELECT device_serial FROM finopsdb.uat_device_allowlist)
    ELSE
      array_join(array_intersect(
        split(return_serial_list_returned, '\\s+'),
        (SELECT collect_set(device_serial) FROM finopsdb.uat_device_allowlist)
      ), ' ') != ''
  END)
