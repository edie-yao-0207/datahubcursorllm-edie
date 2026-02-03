queries = {
    "Get JSON-encoded on trip metric proportions for every Sunday (meaning metric columns have a full week of data)": """
        select
        org_id,
        to_json(
            daily_rolling_12week_avg_proportion_per_hour
        ) as daily_rolling_12week_avg_proportion_per_hour,
        to_json(daily_rolling_12week_total_trips_per_hour) as daily_rolling_12week_total_trips_per_hour
        from
        dataengineering.fct_org_weekly_trip_trends
        where date(date) - date(week_start) = interval 6 days
    """,
}
