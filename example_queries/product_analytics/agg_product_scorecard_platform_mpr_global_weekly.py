queries = {
    "Get full admin org percentage by region for the week ending 2024-01-07": """
            SELECT
            region,
            SUM(full_admin_orgs) AS full_admin_orgs,
            SUM(total_orgs) AS total_orgs,
            full_admin_orgs / total_orgs AS full_admin_org_percentage
            FROM
                product_analytics.agg_product_scorecard_platform_mpr_global_weekly
            WHERE
                period_end = '2024-01-07'
            GROUP BY 1
            """,
}
