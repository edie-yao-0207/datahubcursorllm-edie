queries = {
    "Get total number of completed routes by country for the month of June 2024": """
            SELECT
            account_billing_country,
            SUM(completed_routes) as completed_routes
            FROM
                product_analytics.agg_product_scorecard_telematics_mpr_global_monthly
            WHERE
            period_end = '2024-06-30'
            GROUP BY 1
            """,
}
