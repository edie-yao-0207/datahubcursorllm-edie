queries = {
    "Number of event reviews grouped by org and event type for the latest date": """
            SELECT
            org_id,
            detection_type,
            SUM(num_reviews) AS total_reviews
            FROM dataengineering.safety_event_review_results_details_region
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2
            """,
}
