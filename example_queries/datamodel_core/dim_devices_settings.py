queries = {
    "All safety events for an example org on a recent date, with the latest device settings for that device": """
        WITH devices_settings AS (
        SELECT
            *
        FROM
            datamodel_core.dim_devices_settings
        WHERE
            org_id = 54272
            AND `date` = date_sub(current_date(), 2)
        )
        SELECT
        fse.*,
        ds.latest_config_date,
        ds.latest_config_time,
        ds.harsh_event_settings,
        ds.audio_alert_configs
        FROM
        datamodel_safety.fct_safety_events fse
        LEFT JOIN devices_settings ds ON fse.`date` = ds.`date`
        AND fse.org_id = ds.org_id
        AND fse.device_id = ds.device_id
        WHERE
        fse.org_id = 54272
        AND fse.`date` = date_sub(current_date(), 2)
        order by
        1
            """
}
