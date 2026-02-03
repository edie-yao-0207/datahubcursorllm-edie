(
  SELECT
    active_date,
    eom,
    fiscal_year,
    quarter,
    eoq,
    device_region,
    'Total' AS product_id,
    'Total' AS name,
    'Fleet All' AS product_family,
    SUM(active_device_days) AS active_device_days
  FROM
    finops.cost_active_devices_daily
  GROUP BY
    active_date,
    eom,
    fiscal_year,
    quarter,
    eoq,
    device_region
  UNION
  SELECT
    active_date,
    eom,
    fiscal_year,
    quarter,
    eoq,
    device_region,
    try_cast(product_id as STRING) as product_id,
    name,
    product_family,
    active_device_days
  FROM
    finops.cost_active_devices_daily
)