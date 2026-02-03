(
  select
    'week' as period,
    date,
    period_start,
    period_end,
    cloud_region,
    region,
    account_size_segment,
    account_arr_segment,
    account_industry,
    industry_vertical,
    is_paid_safety_customer,
    is_paid_telematics_customer,
    is_paid_stce_customer,
    driver_assignment_source,
    attached_device_type,
    driver_assigned_trips,
    total_trips,
    unassigned_trips_with_safety_event,
    unassigned_trips
  from
    datamodel_dev.agg_product_scorecard_safety_mpr_driver_assignment_source_global_weekly
  union all
  select
    'month' as period,
    date_month as date,
    period_start,
    period_end,
    cloud_region,
    region,
    account_size_segment,
    account_arr_segment,
    account_industry,
    industry_vertical,
    is_paid_safety_customer,
    is_paid_telematics_customer,
    is_paid_stce_customer,
    driver_assignment_source,
    attached_device_type,
    driver_assigned_trips,
    total_trips,
    unassigned_trips_with_safety_event,
    unassigned_trips
  from
    datamodel_dev.agg_product_scorecard_safety_mpr_driver_assignment_source_global_monthly
)