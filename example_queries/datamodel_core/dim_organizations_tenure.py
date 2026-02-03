queries = {
    "Tenure (in months) of each organization as of 30 days ago": """
        select
        org_id,
        tenure,
        run_date as tenure_computation_date,
        date_sub(current_date(), 30) as tenure_as_of_date,
        start_date as tenure_range_start_date,
        end_date as tenure_range_end_date
        from
        datamodel_core.dim_organizations_tenure
        where
        date_sub(current_date(), 30) between start_date
        and end_date
        order by
        2 desc
            """
}
