SELECT org_id, sam_number, region
FROM dataengineering.map_org_sam_number_global
WHERE date = (SELECT MAX(date) FROM dataengineering.map_org_sam_number_global)
