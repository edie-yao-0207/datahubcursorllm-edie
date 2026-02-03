queries = {
    "Assigned license count by date, sku, and org separated by devices, drivers, and users": """
    SELECT date,
        sam_number,
        org_id,
        sku,
        COUNT(DISTINCT CASE WHEN entity_type_name = 'device' THEN uuid ELSE NULL END) AS devices_assigned,
        COUNT(DISTINCT CASE WHEN entity_type_name = 'user' THEN uuid ELSE NULL END) AS users_assigned,
        COUNT(DISTINCT CASE WHEN entity_type_name = 'driver' THEN uuid ELSE NULL END) AS drivers_assigned
    FROM datamodel_platform.dim_license_assignment
    WHERE date = (SELECT MAX(date) FROM datamodel_platform.dim_license_assignment)
    AND (end_time >= date OR end_time IS NULL)
    GROUP BY 1,2,3,4
            """
}
