queries = {
    "API request status counts grouped by org, route, method, and status code, over the latest date": """
            SELECT
            org_id,
            route,
            method,
            status_code,
            SUM(num_requests) AS num_requests
            FROM product_analytics.fct_api_usage
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2, 3, 4
            """,
}
