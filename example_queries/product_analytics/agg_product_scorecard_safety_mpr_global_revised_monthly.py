queries = {
    "Get total number of assigned trainings by country for the month of June 2024": """
            SELECT
            account_billing_country,
            SUM(assigned_trainings) as assigned_trainings
            FROM
                product_analytics.agg_product_scorecard_safety_mpr_global_revised_monthly
            WHERE
            period_end = '2024-06-30'
            GROUP BY 1
            """,
}
