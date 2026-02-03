WITH all_users_table as (
  SELECT
    COLLECT_LIST(users.email) as all_users,
    organizations.id as org_id,
    sfdc_accounts.sam_number
  FROM
    clouddb.users_organizations
    JOIN clouddb.users on users.id = users_organizations.user_id
    JOIN clouddb.organizations on users_organizations.organization_id = organizations.id
    JOIN clouddb.org_sfdc_accounts on org_sfdc_accounts.org_id = organizations.id
    JOIN clouddb.sfdc_accounts on org_sfdc_accounts.sfdc_account_id = sfdc_accounts.id
  WHERE
    organizations.internal_type = 0
  GROUP BY
    organizations.id,
    sfdc_accounts.sam_number
)
SELECT
  OwnerEmail as sfdc_poc_email,
  sam_number as cloud_sam_number,
  SAM_Number_Undecorated__c as sfdc_sam_number,
  org_id,
  ARRAY_CONTAINS(all_users, OwnerEmail) as is_poc_email_in_org
FROM
  all_users_table
  LEFT JOIN sfdc_data.sfdc_accounts on sfdc_accounts.SAM_Number_Undecorated__c = all_users_table.sam_number
WHERE
  OwnerEmail IS NOT NULL and OwnerEmail LIKE '%@%'
