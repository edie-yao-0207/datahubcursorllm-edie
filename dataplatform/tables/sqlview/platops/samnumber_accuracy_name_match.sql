SELECT
  DISTINCT
  cloud_name_to_samnumber.id as org_id,
  cloud_name_to_samnumber.name as cloud_name,
  soundex(cloud_name_to_samnumber.name) as cloud_name_soundex,
  sfdc_accounts.name as sfdc_name,
  soundex(sfdc_accounts.name) as sfdc_name_soundex,
  levenshtein(cloud_name_to_samnumber.name, sfdc_accounts.name) as sfdc_name_levenshtein,
  levenshtein(cloud_name_to_samnumber.name, sfdc_accounts.name) / greatest(
    length(cloud_name_to_samnumber.name),
    length(sfdc_accounts.name)
  ) as sfdc_name_levenshtein_percent,
  ns_org_name_to_samnumber.name as ns_name,
  soundex(ns_org_name_to_samnumber.name) as ns_name_soundex,
  levenshtein(
    cloud_name_to_samnumber.name,
    ns_org_name_to_samnumber.name
  ) as ns_name_levenshtein,
  levenshtein(
    cloud_name_to_samnumber.name,
    ns_org_name_to_samnumber.name
  ) / greatest(
    length(cloud_name_to_samnumber.name),
    length(ns_org_name_to_samnumber.name)
  ) as ns_name_levenshtein_percent,
  cloud_name_to_samnumber.sam_number as sam_number
FROM
  platops.org_name_to_samnumber as cloud_name_to_samnumber
  JOIN clouddb.org_sfdc_accounts on org_sfdc_accounts.org_id = cloud_name_to_samnumber.id
  JOIN clouddb.users_organizations on org_sfdc_accounts.org_id = users_organizations.organization_id
  LEFT JOIN sfdc_data.sfdc_accounts as sfdc_accounts ON REPLACE(sfdc_accounts.SAM_Number_Undecorated__c, '-', '') = REPLACE(cloud_name_to_samnumber.sam_number, '-', '')
  LEFT JOIN netsuite_data.org_name_to_samnumber AS ns_org_name_to_samnumber ON REPLACE(ns_org_name_to_samnumber.sam_number, '-', '') = REPLACE(cloud_name_to_samnumber.sam_number, '-', '')
WHERE
  cloud_name_to_samnumber.internal_type = 0
