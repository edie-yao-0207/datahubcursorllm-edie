SELECT DISTINCT
  osa.org_id,
  sc.sam_number
FROM
  clouddb.sfdc_accounts sc
    JOIN clouddb.org_sfdc_accounts osa
      ON osa.sfdc_account_id = sc.id
    JOIN (
      SELECT DISTINCT
        sam_number
      FROM
        edw.silver.dim_customer
      WHERE
        is_stateramp = true
    ) dc
      ON dc.sam_number = sc.sam_number