queries = {
    "Number of Devices, grouped by org and population grouping, over the latest date": """
            SELECT
                org_id
                , grouping_id
                , count(*) as device_count
            FROM
                product_analytics.dim_populations
            WHERE
                date = date_sub(current_date(), 1)
            GROUP BY ALL
            """,
}
