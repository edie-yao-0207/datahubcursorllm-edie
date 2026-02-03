queries = {
    "Get full admin org percentage by country in June 2024": """
            SELECT
            account_billing_country,
            SUM(full_admin_account) AS full_admin_account,
            COUNT(DISTINCT account_id) AS total_account,
            full_admin_account / total_account AS full_admin_org_percentage
            FROM
                product_analytics.agg_product_scorecard_platform_mpr_global_revised_monthly
            WHERE
                period_end = '2024-06-30'
            GROUP BY 1
            """,
}
