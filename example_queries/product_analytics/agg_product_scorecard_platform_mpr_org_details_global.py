queries = {
    "Get percentage of users who are admins for each org on the latest day": """
            SELECT
            org_id,
            admin_users,
            total_users,
            total_users * 1.0 / admin_users AS admin_percentage
            FROM
                product_analytics.agg_product_scorecard_platform_mpr_org_details_global
            WHERE
                date = date_sub(current_date(), 1)
            """,
}
