SELECT
  mp.org_id,
  sf.sam_number,
  mp.date,
  mp.userEmail as user_email,
  COUNT(1) AS num_page_loads
FROM datamodel_platform_silver.stg_cloud_routes mp
LEFT JOIN clouddb.org_sfdc_accounts osf ON mp.org_id = osf.org_id
LEFT JOIN clouddb.sfdc_accounts sf ON osf.sfdc_account_id = sf.id
WHERE
  mp.userEmail NOT LIKE '%samsara%' AND
  mp.date >= ${start_date} AND
  mp.date < ${end_date} AND
  mp.date IS NOT NULL AND
  sf.sam_number IS NOT NULL AND
  mp.userEmail IS NOT NULL
GROUP BY
  mp.org_id,
  sf.sam_number,
  mp.date,
  mp.userEmail
