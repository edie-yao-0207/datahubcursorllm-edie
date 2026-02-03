queries = {
    "Number of Devices on a date (most recent date in table with data)": """
            SELECT
            device_type,
            COUNT(*)
            FROM
            datamodel_core.dim_devices
            WHERE
            date = (
                select
                max(date)
                FROM
                datamodel_core.dim_devices
            )
            GROUP BY
            1
            """,
    "Vehicle Information Associated with VG Devices (yesterday)": """
            SELECT
            associated_vehicle_make,
            associated_vehicle_model,
            associated_vehicle_year,
            associated_vehicle_engine_model,
            associated_vehicle_primary_fuel_type,
            associated_vehicle_secondary_fuel_type,
            associated_vehicle_engine_type,
            associated_vehicle_trim,
            associated_vehicle_engine_manufacturer,
            COUNT(*)
            FROM
            datamodel_core.dim_devices
            WHERE
            date = date_sub(current_date(), 1)
            AND device_type = 'VG - Vehicle Gateway'
            GROUP BY
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9

    """,
    "Percentage Of VG Devices With Connected Camera": """
        SELECT
        COUNT_IF(
            associated_devices ['camera_device_id'] IS NOT NULL
        ) / COUNT(*) AS percentage_of_vg_devices_with_connected_camera
        FROM
        datamodel_core.dim_devices
        WHERE
        date = date_sub(current_date(), 1)
        AND device_type = 'VG - Vehicle Gateway'
    """,
    "Percentage Of CM Devices With Connected VG": """
    SELECT
    COUNT_IF(associated_devices ['vg_device_id'] IS NOT NULL) / COUNT(*) AS percentage_of_cm_devices_with_connected_vg
    FROM
    datamodel_core.dim_devices
    WHERE
    date = date_sub(current_date(), 1)
    AND device_type = 'CM - AI Dash Cam'
    """,
}
