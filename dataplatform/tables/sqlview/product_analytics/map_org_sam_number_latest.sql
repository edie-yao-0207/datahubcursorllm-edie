SELECT org_id, sam_number
FROM datamodel_core.dim_organizations
WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)