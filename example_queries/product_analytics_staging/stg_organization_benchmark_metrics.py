queries = {
    "Find metric values for a given metric/org/lookback_window combination for the latest date": """
    select
        org_id,
        metric_type,
        lookback_window,
        metric_value
    from product_analytics_staging.stg_organization_benchmark_metrics
    where date = date_sub(current_date(), 1)
    """
}
