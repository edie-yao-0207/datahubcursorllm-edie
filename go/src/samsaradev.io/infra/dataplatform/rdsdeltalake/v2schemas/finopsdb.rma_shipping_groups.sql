`_timestamp` TIMESTAMP,
`_filename` STRING,
`_rowid` STRING,
`_op` STRING,
`rma_order_number` STRING,
`sam_number` STRING,
`uuid` STRING,
`created_at` TIMESTAMP,
`shipping_status_enum` SHORT,
`shipping_tracking_numbers_proto` STRUCT<
  `shipping_tracking_number`: ARRAY<STRING>
>,
`_raw_shipping_tracking_numbers_proto` STRING,
`shipping_carrier_enum` SHORT,
`item_fulfillment_id` STRING,
`date` STRING
