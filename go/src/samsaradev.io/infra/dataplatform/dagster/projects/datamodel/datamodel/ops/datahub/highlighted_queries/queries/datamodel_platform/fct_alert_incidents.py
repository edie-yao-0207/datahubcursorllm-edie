queries = {
    "Summary of alert incidents by object type and trigger type": """
        select object_type,
            explode(trigger_types) as trigger_type,
            count(distinct alert_config_uuid) as unique_alerts_configured,
            count(distinct org_id) as unique_organizations,
            count(alert_occurred_at_ms) as total_incidents,
            sum(case when alert_status = 'resolved' then 1 else 0 end) as total_incidents_resolved,
            round(100.0 * sum(case when alert_status = 'resolved' then 1 else 0 end) / count(alert_occurred_at_ms), 2) as pct_resolved
        from datamodel_platform.fct_alert_incidents
        where date = (select max(date) from datamodel_platform.fct_alert_incidents)
        group by 1,2
        order by 3 desc
    """
}
