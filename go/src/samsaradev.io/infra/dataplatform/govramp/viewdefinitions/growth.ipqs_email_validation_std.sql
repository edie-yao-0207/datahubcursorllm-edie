SELECT ievs.*
FROM default.growth.ipqs_email_validation_std ievs
LEFT JOIN default.clouddb.users u ON ievs.email = u.email
LEFT ANTI JOIN (
SELECT DISTINCT uo.user_id
FROM default.clouddb.users_organizations uo
INNER JOIN default.stateramp.stateramp_orgs so
ON uo.organization_id = so.org_id
) stateramp_users
ON u.id = stateramp_users.user_id