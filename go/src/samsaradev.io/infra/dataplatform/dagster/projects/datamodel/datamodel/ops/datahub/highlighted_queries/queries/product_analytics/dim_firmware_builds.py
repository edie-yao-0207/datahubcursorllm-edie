queries = {
    "Number of Devices, grouped by org and build, as of the latest date": """
            SELECT
            org_id,
            build,
            COUNT(*)
            FROM product_analytics.dim_firmware_builds
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2
            """,
}
