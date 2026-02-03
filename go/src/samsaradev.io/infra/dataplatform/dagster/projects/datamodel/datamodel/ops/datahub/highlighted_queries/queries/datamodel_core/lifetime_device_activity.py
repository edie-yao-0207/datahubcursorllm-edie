queries = {
    "Count of Active Devices by device type, for the latest date": """
                            SELECT
                            ld.date,
                            ld.device_type,
                            count(distinct ld.device_id) AS active_devices
                            FROM
                            datamodel_core.lifetime_device_activity ld
                            WHERE
                            ld.date = (
                                SELECT
                                MAX(date)
                                FROM
                                datamodel_core.lifetime_device_activity
                            )
                            AND l1 > 0
                            GROUP BY
                            1,
                            2
                            ORDER BY
                            3 DESC
            """
}
