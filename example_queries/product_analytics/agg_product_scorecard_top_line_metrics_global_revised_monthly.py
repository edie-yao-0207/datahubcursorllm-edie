queries = {
    "Get total number of miles covered by country for the month of June 2024": """
            SELECT
            account_billing_country,
            SUM(miles) as total_miles
            FROM
                product_analytics.agg_product_scorecard_top_line_metrics_global_revised_monthly
            WHERE
            period_end = '2024-06-30'
            GROUP BY 1
            """,
}
