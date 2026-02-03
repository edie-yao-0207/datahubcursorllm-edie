queries = {
    "Find which hour of day a vehicle is most likely to travel by day of week, in the past week": """
        select
            day_of_week,
            max_by(hour_start, count_trips_on_hour),
            max(count_trips_on_hour)
        from
        (
            select
                day_of_week,
                hour_start,
                count(1) as count_trips_on_hour
            from dataengineering.fct_vehicle_hourly_on_trip_buckets
            where date between date_add(current_date(), -6) and current_date()
            group by day_of_week, hour_start
        )
        group by day_of_week;
    """,
}
