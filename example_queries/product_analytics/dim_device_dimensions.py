queries = {
    "Number of Devices, grouped by org and internal type, over the latest date": """
            SELECT
            org_id,
            internal_type,
            COUNT(DISTINCT device_id) AS device_count
            FROM product_analytics.dim_device_dimensions
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2
            """,
}
