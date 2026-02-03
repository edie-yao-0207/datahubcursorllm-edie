SELECT
  org_id,
  date,
  object_id,
  time,
  CONCAT(org_id, '_', object_id, '_', time) AS usage_id
FROM delta.`s3://samsara-eu-kinesisstats-delta-lake/table/deduplicated/osDSatelliteTransmittedData`
GROUP BY ALL
HAVING
  COUNT(value.proto_value.satellite_transmitted_data.original_payload) * 34 > 0
