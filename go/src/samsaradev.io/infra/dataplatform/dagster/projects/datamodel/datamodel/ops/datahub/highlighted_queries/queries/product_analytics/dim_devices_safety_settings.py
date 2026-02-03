queries = {
    "Number of devices that have mobile usage enabled, for the latest date": """
            SELECT
                COUNT(DISTINCT CASE WHEN mobile_usage_enabled THEN vg_device_id END) AS mobile_usage_devices
            FROM
                product_analytics.dim_devices_safety_settings
            WHERE
                date = date_sub(current_date(), 1)
            """,
}
