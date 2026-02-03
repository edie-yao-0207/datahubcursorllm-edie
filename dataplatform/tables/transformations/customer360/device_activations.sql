WITH ft_activated AS (
  SELECT * FROM ( 
    SELECT
      to_date(hb.first_heartbeat_date) AS date,
      sf.sam_number,
      substr(p.name, 0, 2) AS product_type,
      hb.device_id
    FROM dataprep.device_heartbeats_extended hb
    LEFT JOIN definitions.products p ON
      hb.product_id = p.product_id
    LEFT JOIN clouddb.org_sfdc_accounts osf ON
      hb.org_id = osf.org_id
    LEFT JOIN clouddb.sfdc_accounts sf ON
      osf.sfdc_account_id = sf.id
  )
  PIVOT (
    COUNT(DISTINCT device_id)
    FOR product_type IN ('AG', 'VG', 'CM', 'IG') -- only select AG, VG, CM, and IG devices
  )
)

SELECT 
    date,
    sam_number,
    AG AS num_ag_activated,
    VG AS num_vg_activated,
    CM AS num_cm_activated,
    IG AS num_ig_activated
FROM ft_activated
WHERE
  date >= ${start_date} AND
  date < ${end_date} AND
  date IS NOT NULL AND
  sam_number IS NOT NULL
