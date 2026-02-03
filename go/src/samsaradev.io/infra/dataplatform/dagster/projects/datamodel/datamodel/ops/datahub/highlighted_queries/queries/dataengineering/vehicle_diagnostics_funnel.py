queries = {
    "Find count of diagnostic signals per MMY": """
    select
        make,
        model,
        year,
        signal_name,
        count(1) as device_count
    from
        dataengineering.vehicle_diagnostics_funnel
    where
        date = (select max(date) from dataengineering.vehicle_diagnostics_funnel)
    group by
        all
    """
}
