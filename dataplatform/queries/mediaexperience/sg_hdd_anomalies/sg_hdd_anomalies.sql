SELECT DISTINCT
  organizations.id AS org_id,
  organizations.name AS org_name,
  object_id AS device_id,
  'https://cloud.samsara.com/o/' || organizations.id || '/workforce/devices/' || object_id || '/show' AS gateway_link,
  MAX(date) = current_date() AS anomaly_active,
  MAX(date) = current_date() AND MIN(date) = current_date() AS anomaly_new,
  collect_set(value.proto_value.anomaly_event.description) AS anomaly_descriptions
FROM
  kinesisstats.osdanomalyevent
  JOIN clouddb.organizations ON osdanomalyevent.org_id = organizations.id
  JOIN productsdb.devices ON osdanomalyevent.object_id = devices.id
WHERE
  date >= date_sub(current_date(), 7)
  -- product_id 55 = SG1 Site Gateway
  AND devices.product_id = 55
  -- internal_type 0 = customer
  AND organizations.internal_type = 0
  AND (
    value.proto_value.anomaly_event.description LIKE 'Drive not found%'
    OR value.proto_value.anomaly_event.description LIKE 'panic: write%no space left on device%'
  )
GROUP BY
  organizations.id,
  org_name,
  object_id
ORDER BY
  anomaly_new DESC,
  anomaly_active DESC,
  org_name
