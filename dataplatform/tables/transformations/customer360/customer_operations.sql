WITH customer_dates AS (
    SELECT
        cd.date,
        cd.sam_number
    FROM customer360.org_dates cd
    GROUP BY 
      cd.date,
      cd.sam_number
),

devices_returned AS (
    SELECT
        transactiON_date AS date,
        sam_number,
        product,
        COUNT(distinct cASe when product like '%VG%' then serial end) AS number_returned_vg,
        COUNT(distinct cASe when product like '%CM%' then serial end) AS number_returned_cm,
        COUNT(distinct cASe when product like '%AG%' then serial end) AS number_returned_ag,
        COUNT(distinct cASe when product like '%IG%' then serial end) AS number_returned_ig,
        COUNT(distinct cASe when product like '%SC%' then serial end) AS number_returned_sc,
        COUNT(distinct cASe when product like '%SG%' then serial end) AS number_returned_sg,
        COUNT(distinct cASe when product like '%EM%' then serial end) AS number_returned_em
    FROM dataprep.devices_sold devices_returned
    WHERE transactiON_type = 'Item Receipt'
    group by transactiON_date, sam_number, product
),

daily_increments AS (
    SELECT 
        DISTINCT date 
    FROM customer_dates
),

device_id_and_names AS (
    SELECT
        t1.id AS device_id, 
        t2.name 
    FROM productsdb.devices t1
    LEFT JOIN definitions.products t2
        ON t1.product_id = t2.product_id
),

AGs_CMs_VGs as (
    SELECT
        t1.*
    FROM dataprep.device_heartbeats t1
    JOIN device_id_and_names t2
        ON t1.device_id = t2.device_id
    WHERE substr(t2.name, 0, 2) IN ('AG', 'VG', 'CM', 'SC', 'SG','EM')
),

total_devices_connected_over_time as (
    SELECT
        t1.date,
        t2.org_id,
        COUNT(DISTINCT t2.device_id) AS total_devices_connected
    FROM daily_increments t1
    LEFT JOIN AGs_CMs_VGs t2
        ON t2.first_heartbeat_date <= t1.date
    GROUP BY 
        t1.date, 
        t2.org_id
),

total_devices_connected_over_time_sam AS (
    SELECT 
        t1.org_id, 
        t2.sam_number, 
        t1.date, 
        t1.total_devices_connected
    FROM total_devices_connected_over_time t1
    LEFT JOIN dataprep.customer_metadata t2
        ON t1.org_id = t2.org_id
    JOIN clouddb.organizations t3
        ON t1.org_id = t3.id
    WHERE t3.internal_type = 0
),
total_devices_connected_over_time_per_sam AS (
    SELECT
        sam_number, 
        date, 
        SUM(total_devices_connected) AS total_devices_connected
    FROM total_devices_connected_over_time_sam 
    GROUP BY 
        sam_number, 
        date
),

total_devices_shipped_over_time_per_sam_no_free_trail AS (
    SELECT
        t2.sam_number, 
        t1.date, 
        COUNT(DISTINCT serial) AS total_devices_shipped
    FROM daily_increments t1
    LEFT JOIN dataprep.devices_sold t2
        ON t2.transaction_date <= t1.date
        AND substr(t2.product, 0, 2) IN ('AG', 'VG', 'CM', 'SC', 'SG','EM')
        AND t2.transaction_type IN ("Item Fulfillment", "Sales Order")
    GROUP BY 
        t1.date, 
        t2.sam_number
),

free_trail_device_counts AS (
    SELECT 
        t1.date, 
        t2.sam_number, 
        SUM(quantity) AS total_free_trail_devices_til_date
    FROM daily_increments t1
    LEFT JOIN dataprep.devices_free_trials t2
        ON t2.date <= t1.date
        AND substr(t2.product, 0, 2) IN ('AG', 'VG', 'CM', 'SC', 'SG','EM')
    GROUP BY 
        t1.date, 
        t2.sam_number
),

total_devices_shipped_over_time_per_sam AS (
    SELECT
        COALESCE(t1.sam_number, t2.sam_number) as sam_number, 
        COALESCE(t1.date, t2.date) as date,
        COALESCE(t1.total_devices_shipped, 0) + COALESCE(t2.total_free_trail_devices_til_date, 0) AS total_devices_shipped
    FROM total_devices_shipped_over_time_per_sam_no_free_trail t1
    FULL JOIN free_trail_device_counts t2
        ON t1.sam_number = t2.sam_number 
        AND t1.date = t2.date
),

total_devices_shipped_and_connected_over_time AS (
    SELECT
        t1.sam_number, 
        t1.date, 
        total_devices_connected, 
        total_devices_shipped
    FROM total_devices_connected_over_time_per_sam t1
    LEFT JOIN total_devices_shipped_over_time_per_sam t2
        ON t1.sam_number = t2.sam_number 
        AND t1.date = t2.date
)

SELECT
    org_dates.sam_number,
    org_dates.date,
    MAX(dr.number_returned_vg) AS num_returned_vg,
    MAX(dr.number_returned_cm) AS num_returned_cm,
    MAX(dr.number_returned_ag) AS num_returned_ag,
    MAX(dr.number_returned_ig) AS num_returned_ig,
    MAX(connected_shipped.total_devices_connected) as total_devices_connected,
    MAX(connected_shipped.total_devices_shipped) as total_devices_shipped,
    MAX(dr.number_returned_sc) AS num_returned_sc,
    MAX(dr.number_returned_sg) AS num_returned_sg,
    MAX(dr.number_returned_em) AS num_returned_em
FROM customer_dates org_dates
LEFT JOIN devices_returned dr
    ON org_dates.date = dr.date
    AND org_dates.sam_number = dr.sam_number
LEFT JOIN total_devices_shipped_and_connected_over_time connected_shipped
    ON org_dates.sam_number = connected_shipped.sam_number
    AND org_dates.date = connected_shipped.date
WHERE
    org_dates.date IS NOT NULL AND
    org_dates.sam_number IS NOT NULL AND
    org_dates.date >= ${start_date} AND
    org_dates.date < ${end_date}
GROUP BY
    org_dates.sam_number,
    org_dates.date
