queries = {
    "Number of orgs that have mobile usage enabled, for the latest date": """
            SELECT
                COUNT(DISTINCT CASE WHEN mobile_usage_enabled THEN org_id END) AS mobile_usage_orgs
            FROM
                product_analytics.dim_organizations_safety_settings
            WHERE
                date = date_sub(current_date(), 1)
            """,
}
