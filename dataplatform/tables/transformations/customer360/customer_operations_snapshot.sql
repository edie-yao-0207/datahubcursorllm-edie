WITH customers AS (
  SELECT 
    DISTINCT sam_number 
  from dataprep.customer_metadata    
),

devices_shipped AS (

  SELECT * FROM ( 
    SELECT 
      sam_number, 
      substr(product, 0, 2) AS product_type,
      serial
    FROM dataprep.devices_sold
  )
  PIVOT (
    COUNT(DISTINCT serial)
    FOR product_type IN ('AG', 'VG', 'CM', 'IG','SG','SC','EM') -- only select AG, VG, and CM devices
  ) 
  
),

devices_purchased AS (

  SELECT * FROM ( 
    SELECT 
      sam_number, 
      substr(product, 0, 2) AS product_type,
      serial
    FROM dataprep.devices_sold
    WHERE order_type LIKE '%Revenue%'
  )
  PIVOT (
    COUNT(DISTINCT serial)
    FOR product_type IN ('AG', 'VG', 'CM', 'IG','SG','SC','EM') -- only select AG, VG, and CM devices
  ) 

)

SELECT 
    c.sam_number,
    ds.VG AS num_vg_shipped,
    ds.CM AS num_cm_shipped,
    ds.AG AS num_ag_shipped,
    ds.IG AS num_ig_shipped,
    dp.VG AS num_vg_purchased,
    dp.CM AS num_cm_purchased,
    dp.AG AS num_ag_purchased,
    dp.IG AS num_ig_purchased,
    ds.SG AS num_sg_shipped,
    ds.SC AS num_sc_shipped,
    ds.EM AS num_em_shipped,
    dp.SG AS num_sg_purchased,
    dp.SC AS num_sc_purchased,
    dp.EM AS num_em_purchased
FROM customers c
LEFT JOIN devices_shipped ds ON c.sam_number = ds.sam_number
LEFT JOIN devices_purchased dp ON c.sam_number = dp.sam_number
WHERE c.sam_number IS NOT NULL
