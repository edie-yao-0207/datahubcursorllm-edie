queries = {
    "Find which orgs had the highest proportion of trips on Wednesdays at 10 AM UTC": """
        select
            org_id,
            sum(rolling_26week_total_on_trip) / sum(rolling_26week_total_on_trip)
        from dataengineering.fct_vehicle_hourly_trip_trends
        where date between date_add(current_date(), -6) and current_date()
            and day_of_week = 'Wednesday'
            and hour_start = '10:00'
        group by org_id
        order by sum(rolling_26week_total_on_trip) / sum(rolling_26week_total_on_trip) desc
        limit 10;
    """,
}
