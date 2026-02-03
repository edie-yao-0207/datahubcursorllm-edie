queries = {
    "Response ratio stats for devices for the latest date": """
            SELECT
            type,
            type_field,
            primary_fields,
            secondary_field,
            org_id,
            device_id,
            obd_value,
            value
            FROM product_analytics.agg_device_stats_secondary_response_ratio
            WHERE date = date_sub(current_date(), 1)
            """,
}
