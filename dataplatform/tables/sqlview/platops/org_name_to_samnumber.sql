SELECT DISTINCT
    organizations.id,
    organizations.name,
    organizations.internal_type,
    sfdc_accounts.sam_number
  FROM
    clouddb.organizations
    INNER JOIN clouddb.org_sfdc_accounts on org_sfdc_accounts.org_id = organizations.id
    INNER JOIN clouddb.sfdc_accounts on sfdc_accounts.id = org_sfdc_accounts.sfdc_account_id
