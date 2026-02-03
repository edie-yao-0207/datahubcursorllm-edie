queries = {
    "Hour with the most anomalies yesterday": """
        SELECT
            date,
            hour_utc,
            COUNT(date)
        FROM product_analytics.agg_osdanomalyevent_hourly
        WHERE date = date_sub(CURRENT_DATE(), 1)
        GROUP BY 1,2
        ORDER BY 3 DESC;
        """,
}
