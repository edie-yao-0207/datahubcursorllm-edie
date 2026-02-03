queries = {
    "Number of routes created on a given date for Jan 2024, including the number of completed and skipped stops for those routes": """
    select
        date,count(distinct route_id) as created_route_count,
        sum(num_stops_by_state['completed']) as completed_stops,
        sum(num_stops_by_state['skipped']) as skipped_stops
    from datamodel_telematics.fct_dispatch_routes
    where date between '2024-01-01' and '2024-01-31'
    group by date
    order by date
            """
}
