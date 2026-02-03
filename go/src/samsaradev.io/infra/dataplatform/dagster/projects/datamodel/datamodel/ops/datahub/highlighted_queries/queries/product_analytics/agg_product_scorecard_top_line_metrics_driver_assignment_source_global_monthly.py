queries = {
    "Get total duration of inward CM assigned trips for the month of June 2024": """
            SELECT
            account_billing_country,
            SUM(inward_assigned_trip_duration) as inward_assigned_trip_duration
            FROM
                product_analytics.agg_product_scorecard_top_line_metrics_driver_assignment_source_global_monthly
            WHERE
            period_end = '2024-06-30'
            GROUP BY 1
            """,
}
