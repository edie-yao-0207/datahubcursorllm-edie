queries = {
    "Get total number of self-coaching events by country for the month of June 2024": """
            SELECT
            account_billing_country,
            SUM(self_coaching_events) as self_coaching_events
            FROM
                product_analytics.agg_product_scorecard_safety_mpr_event_global_monthly
            WHERE
            period_end = '2024-06-30'
            GROUP BY 1
            """,
}
