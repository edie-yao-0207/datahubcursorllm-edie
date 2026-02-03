queries = {
    "Number of SER license values by org, detection type, and region for the latest date": """
            SELECT
            org_id,
            detection_type,
            region,
            has_ser_license,
            COUNT(*)
            FROM dataengineering.safety_event_reviews_details
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2, 3, 4
            """,
}
