queries = {
    "Top 10 organizations by number of drivers (as of most recent date)": """
            select
            do.org_name,
            count(distinct fda.driver_id) as num_drivers
            from
            datamodel_platform.fct_driver_activity fda
            join datamodel_core.dim_organizations do
            on fda.org_id = do.org_id
            and fda.date = do.date
            where
            fda.date = (
                select
                max(date)
                from
                datamodel_platform.fct_driver_activity
            )
            group by
            1
            order by
            2 desc
            limit
            10
        """
}
