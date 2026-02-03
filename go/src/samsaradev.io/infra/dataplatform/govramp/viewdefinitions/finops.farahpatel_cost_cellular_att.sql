(
  SELECT
    billing_month,
    SUM(data_usage) as data_usage,
    COUNT(DISTINCT iccid) as count_active_iccid,
    record_received_date
  FROM
    dataprep_cellular.att_daily_usage
  GROUP BY
    billing_month,
    record_received_date
)