WITH ft_shipments AS (
    SELECT * FROM ( 
        SELECT
        ft.date, 
        ft.sam_number,
        substr(product, 0, 2) AS product_type,
        ft.quantity
        FROM dataprep.devices_free_trials ft
    )
    PIVOT (
        SUM(quantity)
        FOR product_type IN ('AG', 'VG', 'CM', 'IG') -- only select AG, VG, CM, and IG devices
    ) 
)

SELECT 
    date,
    sam_number,
    AG AS num_ag_shipped,
    VG AS num_vg_shipped,
    CM AS num_cm_shipped,
    IG AS num_ig_shipped
FROM ft_shipments
WHERE
    date >= ${start_date} AND
    date < ${end_date} AND
    date IS NOT NULL AND
    sam_number IS NOT NULL
