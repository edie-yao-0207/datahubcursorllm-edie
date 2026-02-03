# Sample Query: Count active devices by product type
# Created as a git tutorial example

QUERY = """
SELECT 
    d.device_type,
    COUNT(DISTINCT CONCAT(d.org_id, '-', d.device_id)) as device_count
FROM datamodel_core.dim_devices d
JOIN datamodel_core.dim_organizations o 
    ON d.org_id = o.org_id AND d.date = o.date
WHERE d.date = (SELECT MAX(date) FROM datamodel_core.dim_devices)
    AND o.is_paid_customer = true
GROUP BY 1
ORDER BY 2 DESC
"""
