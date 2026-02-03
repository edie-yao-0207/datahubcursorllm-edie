SELECT
    date,
    org_id,
    device_id,
    interval_start,
    COALESCE(metered_usage_bytes, 0) AS metered_usage_bytes,
    COALESCE(whitelist_usage_bytes, 0) AS whitelist_usage_bytes
FROM dataplatform_stable_report.metered_usage
    FULL OUTER JOIN dataplatform_stable_report.whitelist_usage
    USING(date, org_id, device_id, interval_start)
WHERE date >= ${start_date}
  AND date < ${end_date}
