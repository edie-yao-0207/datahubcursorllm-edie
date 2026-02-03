SELECT u.*
FROM default.datamodel_platform_bronze.raw_clouddb_users u
LEFT ANTI JOIN (
SELECT DISTINCT uo.user_id
FROM default.clouddb.users_organizations uo
INNER JOIN default.stateramp.stateramp_orgs so
ON uo.organization_id = so.org_id
) stateramp_users
ON u.id = stateramp_users.user_id