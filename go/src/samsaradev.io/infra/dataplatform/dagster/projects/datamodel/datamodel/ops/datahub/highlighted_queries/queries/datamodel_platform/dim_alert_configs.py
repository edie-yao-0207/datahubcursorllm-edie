queries = {
    "Number of alerts (as of most recent date) by active status and primary trigger type": """
        SELECT
        any_value(date) as date,
        deleted_ts_utc is null as active,
        primary_trigger_type,
        COUNT(*) AS n_alerts
        FROM
        datamodel_platform.dim_alert_configs
        WHERE
        date = (
            SELECT
            MAX(date)
            FROM
            datamodel_platform.dim_alert_configs
        )
        GROUP BY
        2,
        3
        ORDER BY
        n_alerts DESC
            """
}
