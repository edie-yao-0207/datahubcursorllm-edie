queries = {
    "Find average rate of harsh acceleration for a specific org": """
    select
        date,
        org_id,
        sum(ms_in_accel_hard_accel) / sum(on_duration_ms) as harsh_acceleration_time_proportion
    from product_analytics.fct_eco_driving_augmented_reports
    where date = current_date()
    and org_id = 32663
    """
}
