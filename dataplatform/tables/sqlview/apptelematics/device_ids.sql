SELECT
  DISTINCT(id)
FROM productsdb.devices
WHERE product_id = 141 -- App Telematics Device Product Id (products/products.goL#1024)
AND org_id != 1  -- Ignore orphaned devices
