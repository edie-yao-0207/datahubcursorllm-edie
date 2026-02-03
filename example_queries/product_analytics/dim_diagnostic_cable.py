queries = {
    "Number of Devices, grouped by org and cable, as of the latest date": """
            SELECT
            org_id,
            cable_id,
            COUNT(*)
            FROM product_analytics.dim_diagnostic_cable
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2
            """,
}
