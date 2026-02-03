queries = {
    "Number of coaching sessions for the latest date, grouped by org and region": """
            SELECT
            org_id,
            region,
            COUNT(*)
            FROM dataengineering.safety_event_coaching_sessions_global
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2
            """,
    "Calculates coaching session duration for the latest date": """
            SELECT
            org_id,
            session_completed_time - session_start_time AS session_duration
            FROM dataengineering.safety_event_coaching_sessions_global
            WHERE date = date_sub(current_date(), 1)
            """,
}
