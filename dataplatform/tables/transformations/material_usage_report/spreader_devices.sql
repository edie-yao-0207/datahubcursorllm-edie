SELECT
  DISTINCT(org_id, device_id) as org_device
FROM
  material_usage_report.spreader_event_intervals
