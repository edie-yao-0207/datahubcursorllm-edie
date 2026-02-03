queries = {
    "Trip details (total duration, total distance) grouped by org and device for a recent date": """
            SELECT
            org_id,
            vg_device_id,
            SUM(duration_mins) AS total_duration_minutes,
            SUM(distance_miles) AS total_distance_miles
            FROM dataengineering.trip_details_global
            WHERE date = date_sub(current_date(), 15)
            GROUP BY 1, 2
            """,
}
