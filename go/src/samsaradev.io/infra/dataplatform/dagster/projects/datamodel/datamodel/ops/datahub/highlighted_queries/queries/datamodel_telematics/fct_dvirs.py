queries = {
    "Current counts by status for DVIRs created on 2024-01-01": """
    select
        dvir_status,
        count(*) as dvir_count
    from datamodel_telematics.fct_dvirs
    where date = '2024-01-01'
    group by 1
            """
}
