queries = {
    "Total Trips and Miles Driven for AGs and VGs Yesterday": """
                            select
                    format_number(sum(t.trip_count), '#,###') as total_trips,
                    format_number(sum(t.total_distance_miles), '#,###') as total_miles

                    from datamodel_telematics.fct_trips_daily t
                    left join datamodel_core.dim_devices d
                        on d.device_id = t.device_id
                        and d.org_id = t.org_id
                        and d.date = t.date
                    left join datamodel_core.dim_organizations o
                        on o.org_id = t.org_id
                        and o.date = t.date
                    left join internaldb.simulated_orgs bo
                        on bo.org_id = t.org_id

                    where t.date = date_sub(current_date, 1) -- looking at yesterday
                    and d.product_id in (24, 53, 178, 125, 68, 84, 140, 27) -- david used these ids
                    and o.internal_type_name = "Customer Org" -- only want customer orgs
                    and bo.org_id is null -- don't want big org ids
            """
}
