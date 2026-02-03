SELECT DISTINCT
    devices.serial,
    sfdc_accounts.sam_number,
    organizations.internal_type,
    organizations.id as org_id
  FROM
    productsdb.devices
    INNER JOIN clouddb.org_sfdc_accounts on devices.org_id = org_sfdc_accounts.org_id
    INNER JOIN clouddb.organizations on devices.org_id = organizations.id
    INNER JOIN clouddb.sfdc_accounts on sfdc_accounts.id = org_sfdc_accounts.sfdc_account_id
    INNER JOIN productsdb.gateways on gateways.serial = devices.serial AND gateways.org_id = devices.org_id AND gateways.device_id = devices.id
