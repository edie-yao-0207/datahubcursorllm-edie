WITH numbered_serial_order_records as (
  SELECT
    CAST(sales_order_transaction_id AS INT) AS sales_order_transaction_id,
    serial_number,
    REPLACE(sam_number, '-', '') AS sam_number,
    order_created_at,

    -- This will add numbers to each row for a given serial ordered by the sales_orders date.
    -- This simplifies selecting the most recent sales order for a given serial.
    -- ex: most recent sales order with for a given serial will have rownum = 1
    ROW_NUMBER() OVER (
      PARTITION BY serial_number
      ORDER BY
        order_created_at DESC
    ) AS rownum
  FROM
    netsuite_data.order_history
)
SELECT DISTINCT
  serial_number,
  sam_number,
  sales_order_transaction_id,
  order_created_at
FROM
  numbered_serial_order_records
WHERE
  rownum = 1 AND serial_number LIKE '%-%'
ORDER BY order_created_at DESC
