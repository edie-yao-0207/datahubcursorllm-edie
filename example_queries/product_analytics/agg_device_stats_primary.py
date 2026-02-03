queries = {
    "Stats for devices for the latest date": """
            SELECT
            type,
            field,
            org_id,
            device_id,
            sum,
            count,
            avg,
            stddev,
            variance,
            mean,
            median,
            mode,
            first,
            last
            FROM product_analytics.agg_device_stats_primary
            WHERE date = date_sub(current_date(), 1)
            """,
}
