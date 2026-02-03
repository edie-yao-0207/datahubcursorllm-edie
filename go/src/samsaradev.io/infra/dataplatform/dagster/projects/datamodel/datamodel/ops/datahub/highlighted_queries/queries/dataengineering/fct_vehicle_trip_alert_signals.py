queries = {
    "Find which orgs have the highest number of anomalous trips": """
        select
            org_id,
            count(1) count_alerts
        from dataengineering.fct_vehicle_trip_alert_signals
        where date between date_add(current_date(), -6) and current_date()
        group by org_id
        order by 2 desc
        limit 10;
    """,
}
