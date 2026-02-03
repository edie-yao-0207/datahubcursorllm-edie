queries = {
    "Breakdown of different statuses by org_id and device_id for the latest date": """
            SELECT
            org_id,
            device_id,
            is_coaching_assigned,
            is_car_viewed,
            is_customer_dismissed,
            is_viewed,
            is_actioned,
            is_auto_coached,
            is_critical_event,
            COUNT(*)
        FROM dataengineering.safety_inbox_events_status_global
        WHERE date = date_sub(current_date(), 1)
        GROUP BY
            org_id,
            device_id,
            is_coaching_assigned,
            is_car_viewed,
            is_customer_dismissed,
            is_viewed,
            is_actioned,
            is_auto_coached,
            is_critical_event
            """,
}
