queries = {
    "Number of dashboard visits per user for the latest date, grouped by URL": """
            SELECT
            user_id,
            accessed_url,
            COUNT(*) AS visits
            FROM dataengineering.cloud_dashboard_user_activity_global
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2
            """,
}
