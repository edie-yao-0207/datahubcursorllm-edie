queries = {
    "Find average rate of cruise control for a specific org": """
    select
        date,
        org_id,
        sum(total_cruise_control_ms) / sum(on_duration_ms) as cruise_control_time_proportion
    from product_analytics.fct_eco_driving_reports
    where date = current_date()
    and org_id = 32663
    """
}
