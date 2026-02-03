queries = {
    "Total number of trips that started on a Jan 1 2024 by vehicles across each org along with the total number of miles driven by those trips": """
    SELECT
    trips.org_id,
    format_number(count(1), '#,###') as total_trips,
    format_number(sum(trips.distance_miles), '#,###') as total_miles
    FROM datamodel_telematics.fct_trips trips
    LEFT JOIN datamodel_core.dim_devices devs
    ON trips.date = devs.date
    AND trips.org_id = devs.org_id
    and trips.device_id = devs.device_id
    WHERE devs.device_type = "VG - Vehicle Gateway"
    AND trips.`date` = '2024-01-01'
    GROUP BY 1
            """
}
