SELECT
    DISTINCT
    mismatch_counts.org_id,
    all_serial_count.sam_number,
    all_serial_count.serial_count as total_serials,
    mismatch_counts.serial_count as mismatched_serials,
    ROUND(
      100 * mismatch_counts.serial_count / all_serial_count.serial_count,
      2
    ) as percent_serial_mismatch
  from
    (
      SELECT
        COUNT(*) as serial_count,
        cloud_serial_to_samnumber.org_id,
        sam_number
      from
        platops.serial_to_samnumber as cloud_serial_to_samnumber
      GROUP BY
        org_id,
        sam_number
      ORDER BY
        1 DESC
    ) as all_serial_count
    JOIN (
      SELECT
        COUNT(*) as serial_count,
        cloud_serial_to_samnumber.org_id
      FROM
        platops.serial_to_samnumber as cloud_serial_to_samnumber
        JOIN clouddb.org_sfdc_accounts on org_sfdc_accounts.org_id = cloud_serial_to_samnumber.org_id
        JOIN clouddb.users_organizations on org_sfdc_accounts.org_id = users_organizations.organization_id
        LEFT JOIN netsuite_data.serial_to_samnumber AS ns_serial_to_samnumber ON REPLACE(cloud_serial_to_samnumber.serial, '-', '') = REPLACE(ns_serial_to_samnumber.serial_number, '-', '')
      WHERE
        REPLACE(cloud_serial_to_samnumber.sam_number, '-', '') != REPLACE(ns_serial_to_samnumber.sam_number, '-', '')
        AND
        cloud_serial_to_samnumber.internal_type = 0
      GROUP BY
        cloud_serial_to_samnumber.org_id
    ) as mismatch_counts ON all_serial_count.org_id = mismatch_counts.org_id
  ORDER BY
    3 DESC
