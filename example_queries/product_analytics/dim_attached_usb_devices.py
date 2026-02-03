queries = {
    "Count of USB devices for each org over the latest date, broken down by cm3x and cm2x": """
            SELECT
            org_id,
            has_cm3x,
            has_cm2x,
            COUNT(*)
            FROM product_analytics.dim_attached_usb_devices
            WHERE date = date_sub(current_date(), 1)
            GROUP BY 1, 2, 3
            """,
}
