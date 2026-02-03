queries = {
    "See breakdown for each Frontiers feature on the latest day": """
            SELECT
                feature,
                org_id,
                org_name,
                org_category,
                account_arr_segment,
                account_size_segment_name,
                account_billing_country,
                region,
                sam_number,
                account_id,
                account_name,
                usage_weekly,
                usage_monthly,
                usage_prior_month,
                usage_weekly_prior_month,
                daily_active_user,
                daily_enabled_users,
                weekly_active_users,
                weekly_enabled_users,
                monthly_active_users,
                monthly_enabled_users,
                org_active_day,
                org_active_week,
                org_active_week_prior_month,
                org_active_month,
                org_active_prior_month
            FROM product_analytics.agg_executive_scorecard_frontiers_filtered_global
            """,
}
