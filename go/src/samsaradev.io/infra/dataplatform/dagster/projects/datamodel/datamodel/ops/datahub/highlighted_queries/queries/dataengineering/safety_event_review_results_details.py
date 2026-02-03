queries = {
    "Number of event reviews grouped by org, event type, and region for the latest date": """
            SELECT
            org_id,
            detection_type,
            region,
            SUM(num_reviews) AS total_reviews
            FROM dataengineering.safety_event_review_results_details
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2, 3
            """,
}
