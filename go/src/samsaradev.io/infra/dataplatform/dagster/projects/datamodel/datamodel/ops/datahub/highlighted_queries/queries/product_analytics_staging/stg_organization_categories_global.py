queries = {
    "Get core org category columns used in Metrics Repo for the latest date": """
    SELECT
        org_id,
        org_name,
        sam_number,
        internal_type,
        avg_mileage,
        region,
        fleet_size,
        industry_vertical_raw AS account_industry,
        industry_vertical,
        fuel_category,
        primary_driving_environment,
        fleet_composition,
        account_size_segment,
        account_arr_segment,
        is_paid_customer,
        is_paid_safety_customer,
        is_paid_stce_customer,
        is_paid_telematics_customer
    FROM product_analytics_staging.stg_organization_categories_global
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
    ORDER BY org_id
    """
}
