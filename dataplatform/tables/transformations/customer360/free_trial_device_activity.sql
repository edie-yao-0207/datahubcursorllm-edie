WITH ft_dates AS (
    SELECT 
     DISTINCT sam_number,
     EXPLODE(SEQUENCE(TO_DATE(free_trial_kit_sent_date), CURRENT_DATE())) AS date
    FROM dataprep.customer_metadata
    WHERE
        has_trial AND
        total_open_opp_expected_revenue <> 0
)

SELECT 
    d.sam_number,
    d.date,
    s.num_ag_shipped,
    s.num_vg_shipped,
    s.num_cm_shipped,
    s.num_ig_shipped,
    da.num_ag_activated,
    da.num_vg_activated,
    da.num_cm_activated,
    da.num_ig_activated
FROM ft_dates d
LEFT JOIN customer360.free_trial_shipments s ON
    d.sam_number = s.sam_number AND
    d.date = s.date
LEFT JOIN customer360.device_activations da ON
    d.sam_number = da.sam_number AND
    d.date = da.date
WHERE
    d.date >= ${start_date} AND
    d.date < ${end_date} AND 
    d.date IS NOT NULL AND
    d.sam_number IS NOT NULL
