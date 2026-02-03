queries = {
    "Get feature usage for latest date": """
            SELECT
            feature,
            COUNT(DISTINCT org_id) AS total_orgs,
            COUNT(DISTINCT CASE WHEN usage_weekly > 0 THEN org_id END) AS orgs_using_feature
            FROM product_analytics.agg_product_usage_global
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1
            """,
}
