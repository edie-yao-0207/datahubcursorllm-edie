-- Camera Connector Health Report small intervals
-- https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4637047/Postmortem+2023-11-29+Camera+Connector+Health+Report+Failed+to+Complete
SELECT (
  SELECT
    ROUND(SUM(CASE WHEN end_ms-start_ms < 1000 THEN 1 ELSE 0 END)*100/COUNT(*), 1) AS auxcam_small_interval_percent
  FROM camera_connector_health.auxcam_recording_intervals
  WHERE
    date >= date_sub(curdate(), 1)
) AS auxcam_small_interval_percent, (
  SELECT
    ROUND(SUM(CASE WHEN end_ms-start_ms < 1000 THEN 1 ELSE 0 END)*100/COUNT(*), 1) AS cc_small_interval_percent
  FROM camera_connector_health.cc_recording_intervals
  WHERE
    date >= date_sub(curdate(), 1)
) AS cc_small_interval_percent
