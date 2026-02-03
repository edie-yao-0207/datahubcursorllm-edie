queries = {
    "Number of Devices for the latest date, grouped by org, region, and last activity date": """
            SELECT
            org_id,
            region,
            last_activity_date,
            COUNT(*)
            FROM dataengineering.device_dormancy_global
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2, 3
            """,
}
