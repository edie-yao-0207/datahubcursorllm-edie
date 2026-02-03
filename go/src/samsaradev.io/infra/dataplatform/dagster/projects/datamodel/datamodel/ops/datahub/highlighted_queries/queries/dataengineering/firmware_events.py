queries = {
    "Number of events and total distance traveled for the latest day, grouped by org ID, device ID, and harsh event type": """
            SELECT org_id,
            device_id,
            harsh_event_type,
            COUNT(DISTINCT event_id) AS event_count,
            SUM(distance_traveled) AS total_distance_traveled
            FROM dataengineering.firmware_events
            WHERE date = (SELECT MAX(date) FROM dataengineering.firmware_events)
            GROUP BY org_id, device_id, harsh_event_type;
            """,
}
