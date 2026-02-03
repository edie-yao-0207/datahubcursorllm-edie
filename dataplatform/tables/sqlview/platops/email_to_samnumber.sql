SELECT DISTINCT
    users.email,
    sfdc_accounts.sam_number,
    organizations.internal_type
  FROM
    clouddb.users
    INNER JOIN clouddb.users_organizations on users_organizations.user_id = users.id
    INNER JOIN clouddb.organizations on organizations.id = users_organizations.organization_id
    INNER JOIN clouddb.org_sfdc_accounts on org_sfdc_accounts.org_id = users_organizations.organization_id
    INNER JOIN clouddb.sfdc_accounts on sfdc_accounts.id = org_sfdc_accounts.sfdc_account_id
