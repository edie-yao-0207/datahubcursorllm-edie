queries = {
    "Video retrieval status grouped by org for the latest date": """
            SELECT
            org_id,
            is_retrieval_successful,
            COUNT(*)
            FROM dataengineering.video_requests_global
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2
            """,
}
