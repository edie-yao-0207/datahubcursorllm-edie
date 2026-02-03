queries = {
    "Number of API events grouped by org, route, and region for the latest date": """
            SELECT
            org_id,
            api_route,
            region,
            COUNT(*)
            FROM dataengineering.safety_api_usage_global
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2, 3
            """,
}
