queries = {
    "Number of HoS logs created on a given date for Jan 2024, including the number of assigned HoS logs": """
    select
    date,
    sum(hos_log_count) as hos_log_count,
    sum(assigned_hos_log_count) as assigned_hos_log_count
    from datamodel_telematics.fct_hos_logs
    where date between '2024-01-01' and '2024-01-31'
    group by date
    order by date
            """
}
