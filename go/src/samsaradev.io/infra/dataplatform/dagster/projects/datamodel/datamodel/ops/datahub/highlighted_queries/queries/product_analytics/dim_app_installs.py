queries = {
    "Count of installs by org over the latest date": """
            SELECT
            org_id,
            COUNT(DISTINCT app_name) AS installs
            FROM product_analytics.dim_app_installs
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1
            """,
}
