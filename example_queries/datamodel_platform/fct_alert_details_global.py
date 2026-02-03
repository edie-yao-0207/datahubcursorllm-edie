queries = {
    "Provides various metrics on alerts (total alerts configured, total orgs represented, total incidents raised, total incidents resolved, total incidents viewed in Mixpanel dashboard), grouped by trigger type": """
        select explode(trigger_types) as trigger_type,
            count(distinct alert_config_uuid) as unique_alerts_configured,
            count(distinct org_id) as unique_organizations,
            count(alert_occurred_at_ms) as total_incidents,
            sum(case when alert_status = 'resolved' then 1 else 0 end) as total_incidents_resolved,
            sum(case when mp_processing_time_ms is not null then 1 else 0 end) as total_incidents_viewed
        from datamodel_platform.fct_alert_details_global
        where date = (select max(date) from datamodel_platform.fct_alert_details_global)
        group by 1
        order by 2 desc
    """
}
