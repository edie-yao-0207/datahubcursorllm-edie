SELECT
  ml.date,
  d.org_id,
  ml.user_id,
  ml.json_params:deviceId as device_id
FROM datastreams_history.mobile_logs ml
JOIN datamodel_core.dim_devices d
  ON d.device_id = ml.json_params:deviceId
  AND d.date = '{PARTITION_START}'
  AND d.org_id = ml.org_id
JOIN final_relevant_orgs rl
  ON ml.org_id = rl.org_id
WHERE ml.event_type = 'FLEET_INSTALLER_COMPLETE_FLOW'
AND ml.date BETWEEN DATE_SUB('{PARTITION_START}', 55) AND '{PARTITION_START}'
