queries = {
    "See total number of drivers submitting DVIRs for the latest day per org": """
            SELECT
            org_id,
            dvir_drivers
            FROM product_analytics.agg_admin_insights
            WHERE date = date_sub(current_date(), 1)
            """,
}
