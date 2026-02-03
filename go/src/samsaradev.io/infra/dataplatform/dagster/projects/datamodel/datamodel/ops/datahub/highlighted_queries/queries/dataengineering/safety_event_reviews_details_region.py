queries = {
    "Number of SER license values by org, detection type for the latest date": """
            SELECT
            org_id,
            detection_type,
            has_ser_license,
            COUNT(*)
            FROM dataengineering.safety_event_reviews_details_region
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2, 3
            """,
}
