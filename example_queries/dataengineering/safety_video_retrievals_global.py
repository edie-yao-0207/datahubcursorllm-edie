queries = {
    "Number of conversion events for latest day, grouped by org ID, is_selection_conversion, is_confirmation_conversion, and is_submitted_conversion": """
            SELECT
            org_id,
            is_selection_conversion,
            is_confirmation_conversion,
            is_submitted_conversion,
            COUNT(*)
            FROM dataengineering.safety_video_retrievals_global
            WHERE date = (SELECT MAX(date) FROM dataengineering.safety_video_retrievals_global)
            GROUP BY 1, 2, 3, 4
            """,
}
