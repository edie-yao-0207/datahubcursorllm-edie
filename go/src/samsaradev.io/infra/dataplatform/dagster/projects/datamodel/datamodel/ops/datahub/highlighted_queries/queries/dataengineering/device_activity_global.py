queries = {
    "Number of Devices in either active/online state for the latest date, grouped by org and region": """
            SELECT
            org_id,
            is_device_active,
            is_device_online,
            region,
            COUNT(*)
            FROM dataengineering.device_activity_global
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2, 3, 4
            """,
}
