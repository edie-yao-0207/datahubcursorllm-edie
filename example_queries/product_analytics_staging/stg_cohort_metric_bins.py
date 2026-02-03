queries = {
    "Find metric bins for a given metric/cohort/lookback_window combination for the latest date": """
    select
        cohort_id,
        metric_type,
        lookback_window,
        bin_min,
        bin_max
    from product_analytics_staging.stg_cohort_metric_bins
    where date = date_sub(current_date(), 1)
    """
}
